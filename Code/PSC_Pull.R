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

`%||%` <- function(a,b) if (is.null(a) || (is.atomic(a) && length(a)==1 && is.na(a))) b else a
FSCPSC_API  <- "https://api.fscpsc.com/searches"
FSCPSC_MIME <- "application/vnd.api+json"

canonicalize_pk <- function(x) as.character(x)

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

validate_pred_rows <- function(code, score, rank, top_k){
  code_chr   <- as.character(code)
  code_ok    <- nzchar(code_chr) & nchar(code_chr) == 4
  score_num  <- suppressWarnings(as.numeric(score))
  score_ok   <- is.finite(score_num)
  rank_int   <- suppressWarnings(as.integer(rank))
  rank_ok    <- !is.na(rank_int) & rank_int %in% seq_len(top_k)
  code_ok & score_ok & rank_ok
}

parse_fscpsc_jsonapi <- function(jj){
  inc <- jj$included %||% list()
  items <- inc[vapply(inc, function(x) is.list(x) && identical(x$type,"product-service-codes"), logical(1))]
  if (!length(items)) return(tibble(code=character(),name=character(),score=double(),rank=integer()))
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
      rel  <- x$relationships$searches
      if (!is.list(rel)) return(tibble(code=character(),score=double()))
      dat  <- rel$data %||% list()
      if (!length(dat)) return(tibble(code=character(),score=double()))
      m    <- dat[[1]]$meta %||% list()
      assoc <- if (!is.null(m$association)) suppressWarnings(as.numeric(m$association)) else NA_real_
      tibble(code=toupper(as.character(x$id %||% "")), score=assoc)
    })) %>% distinct(code,.keep_all=TRUE)
  }
  smap <- full_join(rel_a, rel_b, by="code", suffix=c("_a","_b")) %>% transmute(code, score=coalesce(score_a,score_b))
  out <- left_join(rows, smap, by="code") %>% arrange(desc(score), code)
  if (!nrow(out)) return(tibble(code=character(),name=character(),score=double(),rank=integer()))
  out %>% mutate(rank = row_number())
}

fscpsc_search_psc <- function(query, top_k=5, timeout_sec=25, max_retries=5, contact_email=NULL, client_id=NULL, throttle_sec=0.8){
  q <- as.character(query %||% "")
  if (!nzchar(q)) return(tibble(code=character(),name=character(),score=double(),rank=integer()))
  ua <- if (!is.null(contact_email)) paste0("r-fscpsc/0.1 (", contact_email, ")") else "r-fscpsc/0.1"
  req <- request(FSCPSC_API) %>%
    req_url_query(include="product-service-codes") %>%
    req_headers("Accept"=FSCPSC_MIME,"Content-Type"=FSCPSC_MIME,"User-Agent"=ua) %>%
    req_body_json(list(data=list(type="searches", attributes=list(`search-string`=q))), auto_unbox=TRUE) %>%
    req_timeout(timeout_sec) %>%
    req_retry(max_tries=max_retries, backoff=~ runif(1, 0.5, 1.5)*2^(..attempt-1), is_transient=function(resp) resp_status(resp) %in% c(429,502,503,504))
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

run_pairkey_rerun_from_ds <- function(
    ds,
    subcontracts_expanded,
    pair_key_col   = "pair_key",
    query_col      = "psc_query_250",
    top_k          = 5,
    batch_size     = 60,
    pause_between  = 2.0,
    per_call_sleep = 0.8,
    contact_email  = NULL,
    client_id      = NULL,
    outdir         = here("runs","psc","outputs"),
    parts_dir      = file.path(outdir, "preds_topk_stream_wide_run_pk"),
    delta_dir      = file.path(outdir, "ds_deltas"),
    tag            = "wide_run",
    verbose        = TRUE
){
  if (verbose) cat("building pair_key → normalized query map...\n")
  pk_map <- subcontracts_expanded %>%
    transmute(
      pair_key      = canonicalize_pk(.data[[pair_key_col]]),
      psc_query_250 = normalize_query(.data[[query_col]])
    ) %>%
    filter(nzchar(psc_query_250)) %>%
    distinct(pair_key, psc_query_250)
  
  if (verbose) cat("coercing ds and checking existing predictions by pair_key...\n")
  ds_std <- ds %>%
    transmute(
      pair_key      = canonicalize_pk(.data[[pair_key_col]]),
      psc_query_250 = normalize_query(.data[["psc_query_250"]]),
      rank          = suppressWarnings(as.integer(rank)),
      psc_code      = toupper(as.character(psc_code)),
      psc_name      = as.character(psc_name),
      psc_score     = suppressWarnings(as.numeric(psc_score))
    )
  
  have_preds_pk <- ds_std %>%
    filter(validate_pred_rows(psc_code, psc_score, rank, top_k)) %>%
    distinct(pair_key)
  
  need_pk <- pk_map %>%
    distinct(pair_key) %>%
    anti_join(have_preds_pk, by = "pair_key")
  
  if (verbose){
    cat("total pk in expanded:", n_distinct(pk_map$pair_key), "\n")
    cat("pk with predictions in ds:", nrow(have_preds_pk), "\n")
    cat("NEED pk (to rerun):", nrow(need_pk), "\n")
  }
  if (nrow(need_pk) == 0L){
    if (verbose) cat("nothing to do.\n")
    return(invisible(tibble()))
  }
  
  need_pk_q <- need_pk %>%
    left_join(pk_map, by = "pair_key") %>%
    filter(nzchar(psc_query_250)) %>%
    distinct(pair_key, psc_query_250)
  
  need_queries <- need_pk_q %>% distinct(psc_query_250)
  
  if (verbose){
    cat("unique queries to call:", nrow(need_queries), "\n")
  }
  
  dir_create(parts_dir, recurse = TRUE)
  dir_create(delta_dir, recurse = TRUE)
  
  existing_parts <- fs::dir_ls(parts_dir, glob = "part_*.parquet")
  existing_nums  <- suppressWarnings(as.integer(sub("^part_(\\d{6})\\.parquet$", "\\1", basename(existing_parts))))
  existing_nums  <- existing_nums[!is.na(existing_nums)]
  base_idx <- if (length(existing_nums)) max(existing_nums) else 0L
  if (verbose) cat("resuming part index from:", base_idx, "\n")
  
  batches <- split(need_queries$psc_query_250, ceiling(seq_along(need_queries$psc_query_250) / batch_size))
  
  all_new_rows <- vector("list", length(batches))
  
  for (bi in seq_along(batches)){
    frag_idx  <- base_idx + bi
    frag_path <- file.path(parts_dir, sprintf("part_%06d.parquet", frag_idx))
    batch     <- batches[[bi]]
    
    if (verbose) cat("batch", bi, "of", length(batches), "size:", length(batch), "→", frag_path, "\n")
    
    rows <- lapply(batch, function(q){
      preds <- fscpsc_search_psc(
        q, top_k = top_k,
        contact_email = contact_email,
        client_id     = client_id,
        throttle_sec  = per_call_sleep
      )
      tibble(psc_query_250 = q, .pred = list(preds))
    })
    
    pred_cache <- bind_rows(rows) %>%
      tidyr::unnest(.pred, keep_empty = TRUE) %>%
      transmute(
        psc_query_250,
        rank,
        psc_code  = code,
        psc_name  = name,
        psc_score = score
      )
    
    frag_long <- need_pk_q %>%
      filter(psc_query_250 %in% batch) %>%
      left_join(pred_cache, by = "psc_query_250", relationship = "many-to-many") %>%
      transmute(pair_key, psc_query_250, rank, psc_code, psc_name, psc_score)
    
    arrow::write_parquet(frag_long, frag_path)
    
    all_new_rows[[bi]] <- frag_long
    
    if (pause_between > 0 && bi < length(batches)) Sys.sleep(pause_between)
  }
  
  new_rows <- bind_rows(all_new_rows)
  
  if (nrow(new_rows)){
    ts_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
    delta_path <- file.path(delta_dir, paste0("ds_delta_", ts_str, ".parquet"))
    arrow::write_parquet(new_rows, delta_path)
    if (verbose) cat("wrote delta parquet:", delta_path, "rows:", nrow(new_rows), "\n")
  }
  
  invisible(new_rows)
}

subcontracts_expanded <- readRDS(here("Data","Join_Keys","desc_tbl_deduped.rds")) %>%
  rename(psc_query_250 = q)

ds <- readRDS(here("runs", "psc", "outputs", "ds.rds"))

new_rows <- run_pairkey_rerun_from_ds(
  ds                   = ds,
  subcontracts_expanded = subcontracts_expanded,
  pair_key_col         = "pair_key",
  query_col            = "psc_query_250",
  top_k                = 5,
  batch_size           = 60,
  pause_between        = 2.0,
  per_call_sleep       = 0.8,
  contact_email        = "theresa.rincker@dal.frb.org",
  client_id            = "theresa-rincker",
  outdir               = here("runs","psc","outputs"),
  parts_dir            = file.path(here("runs","psc","outputs"), "preds_topk_stream_wide_run_pk"),
  delta_dir            = file.path(here("runs","psc","outputs"), "ds_deltas"),
  tag                  = "wide_run",
  verbose              = TRUE
)



