#' Convert large sas7bdat file to parquet files
#'
#' @description
#' `sas7bdat_to_parquet()` is developed to convert large sas7bdat files (size >= 100 GB) to several parquet files
#' to construct an arrow datasets using [arrow::open_dataset()].
#' A more detailed demonstration for the function can be found in our paper.
#'
#' @param n_rows An integer. Number of rows. This represents the number of rows in the `.sas7bdat` file.
#' This can be obtained by running `proc contents` statement in SAS or from the data documentation files.
#' Future versions of this function will try to avoid relying on `n_rows`.
#' @param n_chunks An integer. Number of `.parquet` parts (See details).
#' @param original_data_dir A string containing the directory of the `.sas7bdat` file.
#' @param output_dir A string containing the directory of the folder to which the `.parquet` files will be stored.
#'
#' @details
#' Choosing `n_chunks` involves a trade-off between having a reasonable number of parts and a reasonable size for each part.
#' Choosing a low `n_chunks` will lead to each `.parquet` part being very large, which might lead to inefficiencies with dealing with
#' the data. Similarly, high `n_chunks` will lead to a large number of very small `.parquet` files, which can decrease the
#' speed of the function.
#'
#'
#' @return
#' The function should produce multiple `.parquet` files based on the specification of `n_chunks`. The size of each part
#' will depend on the size of the data i.e. `n_rows` and `n_chunks`
#' @export
#'
sas7bdat_to_parquet <- function(n_rows, n_chunks, original_data_dir, output_dir){
  n_rows <- n_rows # This should be taken from SAS proc contents
  n_chunks <- n_chunks
  all_parts_except_last <- n_rows %/% (n_chunks-1)
  last_part <- n_rows %% (n_chunks-1)
  chunks <- c(rep(all_parts_except_last, n_chunks-1), last_part)
  pb <- progress::progress_bar$new(format = "[:bar] :current/:total (:percent) eta: :eta", total = n_chunks)
  pb$tick(0)
  z = 0
  m = 1
  for (i in chunks) {
    test <- rio::import(original_data_dir, n_max = i, skip = z)
    rio::export(test, paste0(output_dir,"/part_",m,".parquet"), format = "parquet")
    # print(glue::glue("Part {m} Done"))
    m = m+1
    z = z+i
    pb$tick(1)
    # print(glue::glue("Z is {z} and I is {i}"))
  }
}

