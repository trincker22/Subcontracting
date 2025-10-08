library(dplyr)
library(tidyr)
library(stringr)
library(tibble)
library(purrr)
library(httr2)
library(fs)
library(arrow)
library(rlang)
library(here)

# ---- Load primary data ----
subcontracts_expanded <- readRDS(here("Data", "subcontracts_expanded.rds"))
wide_path <- "/Users/theresarincker/Documents/GitHub/Subcontracting/runs/psc/outputs/subcontracts_with_preds_top5_wide_wide_run.rds"
subcontracts_wide <- readRDS(wide_path)

# ---- Identify missing predictions ----
missing_queries <- subcontracts_wide %>%
  filter(is.na(psc_code1) | !nzchar(psc_code1)) %>%
  distinct(pair_key, psc_query_250) %>%
  filter(nzchar(psc_query_250))
cat("Missing queries:", nrow(missing_queries), "\n")

# ---- Define helpers ----
`%||%` <- function(a,b) if (is.null(a) || (is.atomic(a) && length(a)==1 && is.na(a))) b else a
FSCPSC_API  <- "https://api.fscpsc.com/searches"
FSCPSC_MIME <- "application/vnd.api+json"

normalize_query <- function(x){
  x <- as.character(x)
  x <- iconv(x, to="UTF-8", sub="")
  x <- stringr::str_squish(x)
  x <- tolower(x)
  vapply(x, function(s){
    if (is.na(s) || s=="") return("")
    b <- charToRaw(s)
    if (length(b) > 250L) rawToChar(b[seq_len(250L)]) else s
  }, character(1))
}

parse_fscpsc_jsonapi <- function(jj){
  inc <- jj$included %||% list()
  items <- inc[vapply(inc, function(x) is.list(x) && identical(x$type,"product-service-codes"), logical(1))]
  if (!length(items)) return(tibble(code=character(),name=character(),score=double()))
  rows <- bind_rows(lapply(items, function(x){
    code <- toupper(as.character(x$id %||% x$attributes$id %||% ""))
    name <- as.character(x$attributes$name %||% x$attributes$title %||% x$attributes$`full-name` %||% NA_character_)
    tibble(code=code, name=name)
  })) %>%
    filter(nchar(code)==4) %>%
    distinct(code, .keep_all=TRUE)
  rel_a <- {
    rel <- jj$data$relationships[["product-service-codes"]]
    if (!is.list(rel)) tibble(code=character(),score=double()) else {
      dat <- rel$data %||% list()
      if (!length(dat)) tibble(code=character(),score=double()) else bind_rows(lapply(dat, function(x){
        meta <- x$meta %||% list(); assoc <- if (!is.null(meta$association)) suppressWarnings(as.numeric(meta$association)) else NA_real_
        tibble(code=toupper(as.character(x$id %||% "")), score=assoc)
      })) %>% distinct(code,.keep_all=TRUE)
    }
  }
  rel_b <- {
    if (!length(items)) tibble(code=character(),score=double()) else bind_rows(lapply(items, function(x){
      rel  <- x$relationships$searches
      if (!is.list(rel)) return(tibble(code=character(),score=double()))
      dat  <- rel$data %||% list()
      if (!length(dat)) return(tibble(code=character(),score=double()))
      m    <- dat[[1]]$meta %||% list()
      assoc <- if (!is.null(m$association)) suppressWarnings(as.numeric(m$association)) else NA_real_
      tibble(code=toupper(as.character(x$id %||% "")), score=assoc)
    })) %>% distinct(code,.keep_all=TRUE)
  }
  smap <- full_join(rel_a, rel_b, by="code", suffix=c("_a","_b")) %>%
    transmute(code, score=coalesce(score_a,score_b))
  left_join(rows, smap, by="code") %>% arrange(desc(score), code)
}

fscpsc_search_psc <- function(query, top_k=5, timeout_sec=25, max_retries=5,
                              contact_email=NULL, client_id=NULL, throttle_sec=0.8){
  q <- as.character(query %||% "")
  if (!nzchar(q)) return(tibble(code=character(),name=character(),score=double(),rank=integer()))
  ua <- if (!is.null(contact_email)) paste0("r-fscpsc/0.1 (", contact_email, ")") else "r-fscpsc/0.1"
  req <- request(FSCPSC_API) %>%
    req_url_query(include="product-service-codes") %>%
    req_headers("Accept"=FSCPSC_MIME,"Content-Type"=FSCPSC_MIME,"User-Agent"=ua) %>%
    req_body_json(list(data=list(type="searches", attributes=list(`search-string`=q))), auto_unbox=TRUE) %>%
    req_timeout(timeout_sec) %>%
    req_retry(max_tries=max_retries,
              backoff=~ runif(1, 0.5, 1.5)*2^(..attempt-1),
              is_transient=function(resp) resp_status(resp) %in% c(429,502,503,504))
  if (!is.null(contact_email)) req <- req %>% req_headers("X-Contact-Email"=contact_email)
  if (!is.null(client_id)) req <- req %>% req_headers("X-Client-Id"=client_id)
  resp <- try(req_perform(req), silent=TRUE)
  if (inherits(resp,"httr2_response") && resp_status(resp) %in% c(200,201)){
    jj <- resp_body_json(resp, simplifyVector=FALSE)
    out <- parse_fscpsc_jsonapi(jj)
    if (nrow(out)) out <- out %>% arrange(desc(score), code) %>% mutate(rank=row_number()) %>% slice_head(n=top_k)
    Sys.sleep(throttle_sec)
    return(out)
  }
  tibble(code=character(),name=character(),score=double(),rank=integer())
}

atomic_saveRDS <- function(obj, path){
  dir_create(path_dir(path), recurse=TRUE)
  tmp <- paste0(path, ".tmp")
  saveRDS(obj, tmp)
  file.rename(tmp, path)
}

run_fscpsc_psc_wide_pk <- function(subcontracts_expanded, missing_queries, pair_key_col="pair_key",
                                   query_col="psc_query_250", top_k=5, batch_size=60,
                                   pause_between=2.0, per_call_sleep=0.8, contact_email=NULL,
                                   client_id=NULL, outdir=here("runs","psc","outputs"), tag="wide_run") {
  
  pk <- rlang::sym(pair_key_col)
  df <- subcontracts_expanded %>%
    mutate(psc_query_250=normalize_query(.data[[query_col]])) %>%
    select(!!pk, everything())
  map_pk <- df %>% distinct(!!pk, psc_query_250)
  
  cache_path <- file.path(outdir, paste0("cache_",tag,".rds"))
  long_out_dir <- file.path(here("runs/psc/outputs/preds_topk_stream_wide_run_pk"))
  dir_create(outdir, recurse=TRUE)
  dir_create(long_out_dir, recurse=TRUE)
  
  cache <- if (file_exists(cache_path)) readRDS(cache_path) else tibble(psc_query_250=character(), .pred=list())
  need <- missing_queries %>% anti_join(cache, by="psc_query_250")
  cat("To rerun:", nrow(need), "\n")
  
  # Continue from max part number
  existing_parts <- fs::dir_ls(long_out_dir, glob="*.parquet")
  existing_nums  <- suppressWarnings(as.integer(sub("^part_(\\d{6})\\.parquet$", "\\1", basename(existing_parts))))
  existing_nums  <- existing_nums[!is.na(existing_nums)]
  base_idx <- if (length(existing_nums)) max(existing_nums) else 0L
  message("Continuing parts at N+1 from base_idx=", base_idx)
  
  if (nrow(need)>0){
    batches <- split(need$psc_query_250, ceiling(seq_along(need$psc_query_250)/batch_size))
    for (bi in seq_along(batches)){
      frag_idx  <- base_idx + bi
      frag_path <- file.path(long_out_dir, sprintf("part_%06d.parquet", frag_idx))
      if (!file_exists(frag_path)){
        batch <- batches[[bi]]
        cat("Running batch", bi, "/", length(batches), " (", length(batch), " queries )\n")
        rows <- lapply(batch, function(q){
          preds <- fscpsc_search_psc(q, top_k=top_k, contact_email=contact_email,
                                     client_id=client_id, throttle_sec=per_call_sleep)
          tibble(psc_query_250=q, .pred=list(preds))
        })
        cache <- bind_rows(cache, bind_rows(rows)) %>% distinct(psc_query_250, .keep_all=TRUE)
        atomic_saveRDS(cache, cache_path)
        pred_cache <- cache %>%
          filter(psc_query_250 %in% batch) %>%
          tidyr::unnest(.pred, keep_empty=TRUE) %>%
          transmute(psc_query_250, rank, psc_code=code, psc_name=name, psc_score=score)
        map_batch <- map_pk %>% filter(psc_query_250 %in% batch)
        frag_long <- map_batch %>%
          left_join(pred_cache, by="psc_query_250", relationship="many-to-many") %>%
          transmute(!!pk, psc_query_250, rank, psc_code, psc_name, psc_score)
        arrow::write_parquet(frag_long, frag_path)
        if (pause_between>0 && bi<length(batches)) Sys.sleep(pause_between)
      }
    }
  }
  cat("Done.\n")
}

# ---- Run only missing ----
res <- run_fscpsc_psc_wide_pk(
  subcontracts_expanded,
  missing_queries,
  pair_key_col="pair_key",
  query_col="psc_query_250",
  top_k=5,
  batch_size=60,
  pause_between=2.0,
  per_call_sleep=0.8,
  contact_email="theresa.rincker@dal.frb.org",
  client_id="theresa-rincker",
  outdir=here("runs","psc","outputs"),
  tag="wide_run"
)




