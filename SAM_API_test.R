library(httr2)
library(dplyr)
# https://open.gsa.gov/api/entity-api/
api_key <- Sys.getenv("SAM_API_KEY")
uei <- "MQTRV3XU9LZ3"

body <- request("https://api.sam.gov/entity-information/v3/entities") |> 
  req_headers(Accept = "application/json") |> # json output format 
  req_url_query(
    api_key = api_key, # inputs: api key, uei. outputs: all entityRegistration cols and all assertion cols 
    ueiSAM = uei,
    includeSections = "entityRegistration,assertions"
  ) |>
  req_perform() |>
  resp_body_json(simplifyVector = FALSE)

entity <- body$entityData[[1]]
registration <- entity$entityRegistration
goods <- entity$assertions$goodsAndServices

naics_results <- bind_rows(goods$naicsList) |>
  transmute(
    uei = registration$ueiSAM,
    legal_business_name = registration$legalBusinessName,
    primary_naics = goods$primaryNaics,
    naics_code = naicsCode,
    naics_description = naicsDescription
  )

naics_results
