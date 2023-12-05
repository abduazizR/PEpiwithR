to_parquet <- function(n_rows, n_chunks, original_data_dir, output_dir){
  n_rows <- n_rows # This should be taken from SAS proc contents
  n_chunks <- n_chunks
  all_parts_except_last <- n_rows %/% (n_chunks-1)
  last_part <- n_rows %% (n_chunks-1)
  chunks <- c(rep(all_parts_except_last, n_chunks-1), last_part)

  pb <- progress::progress_bar$new(format = "[:bar] :current/:total (:percent) eta: :eta", total = n_chunks)
  pb$tick(0)
  z = 0
  m = 1
  if (tolower(tools::file_ext(original_data_dir)) %in% c("sas7bdat", "sav","xpt","dta", "spss", "por","zsav")) {
    for (i in chunks) {
      test <- rio::import(original_data_dir, n_max = i, skip = z)
      rio::export(test, paste0(output_dir,"/part_",m,".parquet"), format = "parquet")
      # print(glue::glue("Part {m} Done"))
      m = m+1
      z = z+i
      pb$tick(1)
      # print(glue::glue("Z is {z} and I is {i}"))
    }
  } else if (tolower(tools::file_ext(original_data_dir)) %in% c("CSV", "tsv", "txt")) {
    col_names <- names(rio::import(original_data_dir),
        nrows = 1,
        skip = 0)
    z = 0
    m = 1
    for (i in chunks) {
      test <- rio::import(
        original_data_dir,
        nrows = i,
        skip = z,
        col.names = col_names
      )
      rio::export(test,
                  paste0(output_dir,"/part_",m,".parquet"),
                  format = "parquet")
      # print(glue::glue("Part {m} Done"))
      m = m+1
      z = z+i
      pb$tick(1)
      # print(glue::glue("Z is {z} and I is {i}"))
    }

  }

}

