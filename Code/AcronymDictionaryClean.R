library(readxl)
library(dplyr)
library(stringr)
library(tidyr)

xlsx_path <- "~/Downloads/acronyms (1).xlsx"


raw <- read_xlsx(xlsx_path, col_names = F)
colname <- names(raw)[which.max(colSums(!is.na(raw)))]
lines <- as.character(raw[[colname]])
lines <- lines[!is.na(lines)]
lines <- str_squish(lines)
lines <- lines[nzchar(lines)]

split_acr <- function(txt) {
  m <- regexec("^(\\S{2,15})\\s+(.+)$", txt)
  r <- regmatches(txt, m)[[1]]
  if (length(r) == 3) {
    acr <- toupper(trimws(r[2]))
    exp <- trimws(r[3])
    return(data.frame(acr = acr, exp = exp, stringsAsFactors = FALSE))
  } else {
    return(data.frame(acr = NA, exp = NA, stringsAsFactors = FALSE))
  }
}

acr_tbl <- bind_rows(lapply(lines, split_acr)) %>%
  filter(!is.na(acr), !is.na(exp), nzchar(acr), nzchar(exp))


acr_tbl_split <- acr_tbl %>%
  mutate(exp = str_split(exp, ";")) %>%
  unnest(exp) %>%
  mutate(exp = str_squish(exp)) %>%
  filter(exp != "")

acr_tbl_split <- distinct(acr_tbl_split, acr, exp)


head(acr_tbl_split, 20)


saveRDS(acr_tbl_split, here("acronymdict.RDS"))
