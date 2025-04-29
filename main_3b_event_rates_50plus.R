library(purrr)

infile = "filepath/dataset_survival.RData"
out_dir = "filepath"

### read in dataset
load(infile)

### restrict participants aged 50+ at index date
dat <- dat[dat$age_index>=50,]

### add a constant to obtain results not broken down (i.e. all participants)
dat$all <- "all"

### list domains to loop over
domains <- c("all", "lc_index",
             "age10_index", "sex_index", "ethnicityg_index", "white_index",
             "gor9d_index", "country_index", "imd_quintile_index",
             "health_conditions_index", "health_conditions_impact_index", "health_status_index",
             "work_status_index", "self_employed_index", "work_sector_index", "soc_major_index",
             "remote_collection_index")

### list exposures to loop over
exposures <- c("all", "lc_index")

### list outcomes to loop over
outcomes <- c("change_sector", "change_soc", "retired")

### set up empty lists for outputs
out1 <- as.list(NULL)
out2 <- as.list(NULL)
out3 <- as.list(NULL)

### store original dataset
orig <- dat
rm(dat); gc()

for(k in 1:length(outcomes)) {     ### loop over outcomes
  
  ### define the outcome
  outcome <- outcomes[k]
  
  ### reset dataset
  dat <- orig
  
  ### find midpoint of intervals for calculating exposure time
  dat$futime <- apply(dat[,c(paste0("t1_", outcome), paste0("t2_", outcome))],
                      MARGIN=1, FUN=median)
  
  ### calculate exposure time on 'per 1,000 person-years' basis
  dat$ptime <- dat$futime / (365.25*1000)
  
  for(j in 1:length(exposures)) {  ### loop over exposures within outcomes
    
    ### define the exposure
    exposure <- exposures[j]
    
    for(i in 1:length(domains)) {  ### loop over domains within exposures within outcomes
      
      ### define the domain
      domain <- domains[i]
      
      ### find total sample size by domain and exposure
      n <- aggregate(x=list(n=dat[[paste0("outcome_", outcome)]]==3),
                     by=list(level=dat[[domain]], exposure=dat[[exposure]]),
                     FUN=length)
      
      ### sum number of events by domain and exposure
      events <- aggregate(x=list(events=dat[[paste0("outcome_", outcome)]]==3),
                          by=list(level=dat[[domain]], exposure=dat[[exposure]]),
                          FUN=sum)
      
      ### sum person-time by domain and exposure
      ptime <- aggregate(x=list(person_time=dat[["ptime"]]),
                         by=list(level=dat[[domain]], exposure=dat[[exposure]]),
                         FUN=sum)
      
      ### combine sample size, events and person-time
      comb <- merge(x=n, y=events, all=TRUE)
      comb <- merge(x=comb, y=ptime, all=TRUE)
      comb$pct <- comb$events/comb$n * 100
      comb$outcome <- outcome
      comb$domain <- domain
      comb <- comb[c(7:8,1:2,4,3,6,5)]
      
      out1[[i]] <- comb
      
    }
    
    ### convert lists to a single data.frame by stacking outputs
    df1 <- out1[[1]]
    if(length(domains)>1) {
      for(i in 2:length(domains)) {df1 <- rbind(df1, out1[[i]])}
    }
    
    out2[[j]] <- df1
    
  }
  
  df2 <- out2[[1]]
  if(length(exposures)>1) {
    for(j in 2:length(exposures)) {df2 <- rbind(df2, out2[[j]])}
  }
  
  out3[[k]] <- df2
  
}

df3 <- out3[[1]]
if(length(outcomes)>1) {
  for(k in 2:length(outcomes)) {df3 <- rbind(df3, out3[[k]])}
}

### for each outcome-exposure-domain combination, calculate event rates per
### 1,000 person-years
df3$rate <- unlist(map2(df3$events,
                        df3$person_time,
                        ~poisson.test(.x,.y)$estimate))

### calculate Poisson CI for event rates - lower limit
df3$lcl <- unlist(map2(df3$events,
                       df3$person_time,
                       ~poisson.test(.x,.y)$conf.int[1]))

### calculate Poisson CI for event rates - upper limit
df3$ucl <- unlist(map2(df3$events,
                       df3$person_time,
                       ~poisson.test(.x,.y)$conf.int[2]))

### save results to working directory
write.csv(df3,
          file=paste0(out_dir, "/event_counts_and_rates_50plus.csv"),
          row.names=FALSE)
