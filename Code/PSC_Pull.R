

library(dplyr)
library(tidyr)
library(stringr)
library(tibble)
library(purrr)
library(httr2)
library(fs)
library(arrow)
library(rlang)


subcontracts_expanded <- readRDS("runs/subcontracts_250query.rds")
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
  })) %>% filter(nchar(code)==4) %>% distinct(code, .keep_all=TRUE)
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
      rel  <- x$relationships$searches; if (!is.list(rel)) return(tibble(code=character(),score=double()))
      dat  <- rel$data %||% list(); if (!length(dat)) return(tibble(code=character(),score=double()))
      m    <- dat[[1]]$meta %||% list(); assoc <- if (!is.null(m$association)) suppressWarnings(as.numeric(m$association)) else NA_real_
      tibble(code=toupper(as.character(x$id %||% "")), score=assoc)
    })) %>% distinct(code,.keep_all=TRUE)
  }
  smap <- full_join(rel_a, rel_b, by="code", suffix=c("_a","_b")) %>% transmute(code, score=coalesce(score_a,score_b))
  left_join(rows, smap, by="code") %>% arrange(desc(score), code)
}

fscpsc_search_psc <- function(query, top_k=5, timeout_sec=25, max_retries=5, contact_email=NULL, client_id=NULL, throttle_sec=0.8){
  q <- as.character(query %||% ""); if (!nzchar(q)) return(tibble(code=character(),name=character(),score=double(),rank=integer()))
  ua <- if (!is.null(contact_email)) paste0("r-fscpsc/0.1 (", contact_email, ")") else "r-fscpsc/0.1"
  req <- request(FSCPSC_API) %>%
    req_url_query(include="product-service-codes") %>%
    req_headers("Accept"=FSCPSC_MIME,"Content-Type"=FSCPSC_MIME,"User-Agent"=ua) %>%
    req_body_json(list(data=list(type="searches", attributes=list(`search-string`=q))), auto_unbox=TRUE) %>%
    req_timeout(timeout_sec) %>%
    req_retry(max_tries = max_retries, backoff = ~ runif(1, 0.5, 1.5) * 2^(..attempt - 1), is_transient = function(resp) resp_status(resp) %in% c(429,502,503,504))
  if (!is.null(contact_email)) req <- req %>% req_headers("X-Contact-Email"=contact_email)
  if (!is.null(client_id))    req <- req %>% req_headers("X-Client-Id"=client_id)
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

run_fscpsc_psc_wide_pk <- function(subcontracts_expanded, pair_key_col="pair_key", query_col="psc_query_250", top_k=5, batch_size=60, pause_between=2.0, per_call_sleep=0.8, contact_email=NULL, client_id=NULL, outdir=here::here("runs","psc","outputs"), tag="test"){
  pk <- rlang::sym(pair_key_col)
  df <- subcontracts_expanded %>% mutate(psc_query_250=normalize_query(.data[[query_col]])) %>% select(!!pk, everything())
  map_pk <- df %>% distinct(!!pk, psc_query_250)
  uq <- map_pk %>% distinct(psc_query_250) %>% filter(nzchar(psc_query_250))
  cache_path <- file.path(outdir, paste0("cache_",tag,".rds"))
  long_out_dir <- file.path(outdir, paste0("preds_topk_stream_",tag,"_pk"))
  if (!dir_exists(outdir)) dir_create(outdir, recurse=TRUE)
  if (!dir_exists(long_out_dir)) dir_create(long_out_dir, recurse=TRUE)
  cache <- if (file_exists(cache_path)) readRDS(cache_path) else tibble(psc_query_250=character(), .pred=list())
  need <- uq %>% anti_join(cache, by="psc_query_250")
  if (nrow(need)>0){
    batches <- split(need$psc_query_250, ceiling(seq_along(need$psc_query_250)/batch_size))
    for (bi in seq_along(batches)){
      frag_path <- file.path(long_out_dir, sprintf("part_%06d.parquet", bi))
      if (!file_exists(frag_path)){
        batch <- batches[[bi]]
        rows <- lapply(batch, function(q){
          preds <- fscpsc_search_psc(q, top_k=top_k, timeout_sec=25, max_retries=5, contact_email=contact_email, client_id=client_id, throttle_sec=per_call_sleep)
          tibble(psc_query_250=q, .pred=list(preds))
        })
        cache <- bind_rows(cache, bind_rows(rows)) %>% distinct(psc_query_250, .keep_all=TRUE)
        atomic_saveRDS(cache, cache_path)
        pred_cache <- cache %>% filter(psc_query_250 %in% batch) %>% tidyr::unnest(.pred, keep_empty=TRUE) %>% transmute(psc_query_250, rank, psc_code=code, psc_name=name, psc_score=score)
        map_batch <- map_pk %>% filter(psc_query_250 %in% batch)
        frag_long <- map_batch %>% left_join(pred_cache, by="psc_query_250") %>% transmute(!!pk, psc_query_250, rank, psc_code, psc_name, psc_score)
        arrow::write_parquet(frag_long, frag_path)
        if (pause_between>0 && bi<length(batches)) Sys.sleep(pause_between)
      }
    }
  }
  existing <- if (length(dir_ls(long_out_dir, glob="*.parquet"))>0) arrow::open_dataset(long_out_dir, format="parquet") %>% select(psc_query_250) %>% distinct() %>% collect() %>% pull(psc_query_250) else character(0)
  if (file_exists(cache_path)){
    cache <- readRDS(cache_path)
    missing_q <- setdiff(cache$psc_query_250, existing)
    if (length(missing_q)>0){
      pred_cache <- cache %>% filter(psc_query_250 %in% missing_q) %>% tidyr::unnest(.pred, keep_empty=TRUE) %>% transmute(psc_query_250, rank, psc_code=code, psc_name=name, psc_score=score)
      map_missing <- map_pk %>% filter(psc_query_250 %in% missing_q)
      backfill <- map_missing %>% left_join(pred_cache, by="psc_query_250") %>% transmute(!!pk, psc_query_250, rank, psc_code, psc_name, psc_score)
      arrow::write_parquet(backfill, file.path(long_out_dir, sprintf("part_backfill_%s.parquet", format(Sys.time(), "%Y%m%d%H%M%S"))))
    }
  }
  preds_long <- arrow::open_dataset(long_out_dir, format="parquet") %>% collect() %>% distinct(!!pk, rank, psc_code, .keep_all=TRUE)
  preds_wide <- preds_long %>%
    filter(!is.na(rank), rank<=top_k) %>%
    mutate(rank=as.integer(rank)) %>%
    pivot_wider(
      id_cols = all_of(pair_key_col),
      names_from = rank,
      values_from = c(psc_code, psc_name, psc_score),
      values_fn = list(psc_code=dplyr::first, psc_name=dplyr::first, psc_score=dplyr::first)
    ) %>%
    rename_with(~gsub("psc_code_","psc_code",.), starts_with("psc_code_")) %>%
    rename_with(~gsub("psc_name_","psc_name",.), starts_with("psc_name_")) %>%
    rename_with(~gsub("psc_score_","score",.),  starts_with("psc_score_"))
  for (k in seq_len(top_k)){
    if (!paste0("psc_code",k) %in% names(preds_wide)) preds_wide[[paste0("psc_code",k)]] <- NA_character_
    if (!paste0("psc_name",k) %in% names(preds_wide)) preds_wide[[paste0("psc_name",k)]] <- NA_character_
    if (!paste0("score",k) %in% names(preds_wide)) preds_wide[[paste0("score",k)]] <- NA_real_
    rk <- paste0("rank",k)
    if (!rk %in% names(preds_wide)) preds_wide[[rk]] <- NA_integer_
    preds_wide[[rk]] <- ifelse(!is.na(preds_wide[[paste0("psc_code",k)]]), k, NA_integer_)
  }
  out_wide <- df %>% left_join(preds_wide, by=setNames("pair_key", pair_key_col))
  atomic_saveRDS(out_wide, file.path(outdir, paste0("subcontracts_with_preds_top", top_k, "_wide_", tag, ".rds")))
  list(wide=out_wide, long_dir=long_out_dir, cache_path=cache_path)
}





res <- run_fscpsc_psc_wide_pk(
  subcontracts_expanded,
  pair_key_col="pair_key",
  query_col="psc_query_250",
  top_k=5,
  batch_size=60,
  pause_between=2.0,
  per_call_sleep=0.8,
  contact_email="theresa.rincker@dal.frb.org",
  client_id="theresa-rincker",
  outdir=here::here("runs","psc","outputs"),
  tag="wide_run"
)
