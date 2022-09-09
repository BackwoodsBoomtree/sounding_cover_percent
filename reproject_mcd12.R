library(terra)

mcd12_file <- "G:/MCD12C1/2020/MCD12C1.A2020001.006.2021362215328.hdf"

sds(mcd12_file)

## Majority Land Cover type for IBGP
mcd12_majority    <- rast(mcd12_file, subds = 1)

mcd12_majority_re <- project(mcd12_majority, "+proj=longlat +datum=WGS84", method = "near")

writeRaster(mcd12_majority_re, "G:/MCD12C1/2020/reprocessed/majority/MCD12C1.A2020001.006.Majority_LC.tif", overwrite = TRUE)

## Percent cover rasters
mcd12_percent    <- rast(mcd12_file, subds = 3)

for(i in 1:nlyr(mcd12_percent)) {
  out_file <- project(mcd12_percent[[i]], "+proj=longlat +datum=WGS84")
  
  writeRaster(out_file, paste0("G:/MCD12C1/2020/reprocessed/percent/MCD12C1.A2020001.006.Percent_LC_", sprintf("%02d", i), ".tif"))
}