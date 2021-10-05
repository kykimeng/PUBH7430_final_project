library(tidyverse)
library(magrittr)

# in the childK5p.dat file
# there are 490698 rows of raw data
# these correspond to 18,174 distinct rows of data 
# as the 490698 rows are made up of 27 row chunks 
# each of the 27 row chunks is one row of the actual survey data

con <- file("../data/InstallationFiles/childK5p.dat", "r")
raw_lines <- readLines(con)
close(con)

# number of input rows 
(n <- length(raw_lines))
(n_obs <- n / 27)

# read in the metadata file
metadata <- readLines("../data/InstallationFiles/ECLSK2011_K5PUF.dct")

# for every first row of the 27 row chunks, use the metadata associated with _line(1)
tmp <- lapply(
  1:27, 
  function(i) {
    start_line <- which(str_detect(metadata, paste0("line\\(", i, "\\)")))
    if (i == 27) {
      end_line <- length(metadata) - 1
    } else {
      end_line <- which(str_detect(metadata, paste0("line\\(", i + 1, "\\)")))
    }
    curr_meta <- metadata[(start_line + 1):end_line]
    
    curr_meta <- bind_rows(
      lapply(
        1:length(curr_meta), 
        function(i) {
          read.table(textConnection(curr_meta[[i]]))
        }
      )
    )
    curr_meta %<>% 
      mutate(col_width = str_remove_all(V4, "\\%|[a-z]"), 
             col_width = str_remove_all(col_width, "\\.[0-9]$")) %>% 
      rename(col_name = V3) %>% 
      select(col_name, col_width) %>% 
      na.omit()
    
    # get all rows corresponding to the ith rows in each 27 row chunk 
    curr_rows <- i + (0:(n_obs-1))*27
    fileConn <- file(paste0("output_", i, ".txt"))
    writeLines(raw_lines[curr_rows], fileConn)
    close(fileConn)
    
    curr_data <- read_fwf(
      paste0("output_", i, ".txt"), 
      col_positions = fwf_widths(
        widths = as.numeric(curr_meta$col_width), 
        col_names = curr_meta$col_name
      )
    )
    return(curr_data)
  }
)

all_data <- bind_cols(tmp)
write_csv(all_data, "../data/childk5p.csv")
saveRDS(all_data, "../data/childk5p.rds")

