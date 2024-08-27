library(terra)
library(ncdf4)
library(parallel)

tropomi_dir    <- "/mnt/g/TROPOMI/new/2022"
mcd12_dir      <- "/mnt/g/MCD12C1/v061/2022/reprocessed/percent"
mcd12_majority <- "/mnt/g/MCD12C1/v061/2022/reprocessed/majority/MCD12C1.A2022001.061.Majority_LC.tif"
year           <- 2022

# File lists
tropomi_list <- list.files(tropomi_dir, recursive = TRUE, full.names = TRUE, pattern = "*.nc")
mcd12_list   <- list.files(mcd12_dir, recursive = TRUE, full.names = TRUE, pattern = "*.tif")

# Get Majority raster
lc_maj  <- rast(mcd12_majority)

add_lc <- function(tropomi_file){
  
  t_start <- Sys.time()
  
  print(paste0("Adding land cover and % variables to: ", tropomi_file))
  
  # Get TROPO coords
  myfile <- nc_open(tropomi_file)
  lon    <- ncvar_get(myfile, varid = "PRODUCT/longitude")
  lat    <- ncvar_get(myfile, varid = "PRODUCT/latitude")
  nc_close(myfile)
  
  # Create points, extract majority lc, add majority lc to spatial vector for subsetting
  lonlat     <- cbind(lon, lat)
  pts        <- vect(lonlat, crs = "+proj=longlat +datum=WGS84")
  pts_lc_maj <- extract(lc_maj, pts)
  pts        <- vect(lonlat, atts = pts_lc_maj, crs = "+proj=longlat +datum=WGS84")
  names(pts) <- c("ID", "Majority_Cover")
  
  # Subset points by LC type, extract % cover for that type, then merge into dataframe
  for (i in 1:17) {
    
    sub         <- subset(pts, pts$Majority_Cover == (i - 1))
    lc_perc     <- rast(mcd12_list[i])
    ext_lc_perc <- extract(lc_perc, sub)
    
    # ID gets reset, so assign it back
    ext_lc_perc$ID           <- sub$ID
    colnames(ext_lc_perc)[2] <- "Majority_Cover_Percent"
    
    if (i == 1) {
      all_lc_perc <- ext_lc_perc
    } else {
      all_lc_perc <- rbind(all_lc_perc, ext_lc_perc)
    }
  }
  
  # Merge majority classes and percent into same df
  pts_lc_maj <- merge(pts_lc_maj, all_lc_perc, by = "ID", all = TRUE)
  
  # remove
  rm(all_lc_perc, ext_lc_perc, lc_maj, pts, lon, lat, lc_perc, sub, lonlat)
  gc()
  
  # Write to file
  myfile   <- nc_open(tropomi_file, write = TRUE)
  elem_dim <- myfile$dim[['n_elem']]
  
  lc_name  <- paste0("PRODUCT/LC_MASK_", year)
  lc_perc  <- paste0("PRODUCT/LC_PERC_", year)
  
  # If variables already exist overwrite. Else create and write.
  if (lc_name %in% names(myfile$var)) {
    
    ncvar_put(myfile, lc_name,  pts_lc_maj[,2])
    
  } else {
    lc_var <- ncvar_def(lc_name, units ="-", dim = elem_dim, longname = "Majority Land Cover Type MCD12C1 061 2022",
                        pre = "float", compression = 4, missval = -9999)
    ncvar_add(myfile, lc_var)
    nc_close(myfile)
    myfile <- nc_open(tropomi_file, write = TRUE)
    ncvar_put(myfile, lc_var, pts_lc_maj[,2])
  }
  
  if (lc_perc %in% names(myfile$var)) {
    
    ncvar_put(myfile, lc_perc,  pts_lc_maj[,3])
    
  } else {
    lc_perc_var <- ncvar_def(lc_perc, units ="Percent", dim = elem_dim, longname = "Majority Land Cover Percentage MCD12C1 061 2022",
                             pre = "float", compression = 4, missval = -9999)
    ncvar_add(myfile, lc_perc_var)
    nc_close(myfile)
    myfile <- nc_open(tropomi_file, write = TRUE)
    ncvar_put(myfile, lc_perc_var, pts_lc_maj[,3])
  }
  
  nc_close(myfile)
  
  t_end <- Sys.time()
  
  print(paste0("Done with ", basename(tropomi_file), ". ", (t_end - t_start)))
  
}

mclapply(tropomi_list, add_lc, mc.cores = 5, mc.preschedule = FALSE)