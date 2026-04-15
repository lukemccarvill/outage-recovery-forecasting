#get POUS damage
if(TRUE){
  fileList <- list.files(paste0(wd,'/data/POUS/storm_clusters'),pattern = 'events.gpq',recursive = TRUE)
  data_pous <- data.frame()
  for (ff in 1:length(fileList)){
    #ff<- 1
    data_pous_event1 <- arrow::read_parquet(paste0(wd,'/data/pous/storm_clusters/',fileList[ff]))
    data_pous_event1$storm <- strsplit(fileList[ff],'/')[[1]][1]
    data_pous <- rbind(data_pous,data_pous_event1)  
  }
  
  #pous format
  if(TRUE){
    data_pous$duration_days <- data_pous$duration_hours/24
    data_pous <- data_pous[which(data_pous$duration_days>=1),]
    data_pous$year <- format(as.Date(data_pous$event_start, format="%Y-%m-%d %h:%m:%s"),"%Y")
    data_pous$month <- format(as.Date(data_pous$event_start, format="%Y-%m-%d %h:%m:%s"),"%m")
  }
  
  
  output <- data_pous[,c( "event_start","CountyFIPS","county_pop",
                         "pre_outage_tracked_customers","days_since_data_start","duration_hours",              
                         "n_periods","integral","pop_hours_supply_lost",      
                         "storm", "duration_days","year","month")]
  write.csv(output,paste0(wd,'/result/','POUS.csv'),row.names=FALSE)
}




