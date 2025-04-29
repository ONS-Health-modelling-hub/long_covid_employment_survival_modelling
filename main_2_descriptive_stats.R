source("filepath/cov_dist_cont.R")
source("filepath/cov_dist_cat.R")

infile = "filepath/dataset_survival.RData"
out_dir = "filepath/LC employment survival analysis"

### read in dataset
load(infile)

### covariate distributions - continuous variables
cov_dist_cont <- cov.dist.cont(
  vars = c("age_index"),
  dataset = dat,
  exposure = "lc_index"
)

write.csv(cov_dist_cont,
          file=paste0(out_dir, "/cov_dist_cont_all.csv"),
          row.names=FALSE)

### covariate distributions - categorical variables
cov_dist_cat <- cov.dist.cat(
  vars = c("age10_index", "sex_index", "ethnicityg_index", "white_index",
           "gor9d_index", "country_index", "imd_quintile_index",
           "health_conditions_index", "health_conditions_impact_index", "health_status_index",
           "work_status_index", "self_employed_index", "work_sector_index", "soc_major_index",
           "remote_collection_index"),
  dataset = dat,
  exposure = "lc_index"
)

write.csv(cov_dist_cat,
          file=paste0(out_dir, "/cov_dist_cat_all.csv"),
          row.names=FALSE)

outcomes = c("change_sector", "change_soc", "retired")

### loop over outcome variables
for(i in 1:length(outcomes)) {
  
  ### select outcome
  outcome <- outcomes[i]
  
  ### set up time-to-event variables
  t1_var <- paste0("t1_", outcome)
  t2_var <- paste0("t2_", outcome)
  
  ### calculate follow-up time from index date to midpoint of interval
  dat$futime <- apply(dat[,c(t1_var, t2_var)], 1, FUN=median)
  
  ### calculate follow-up time stats by LC status
  futime_stats_all <- c(as.numeric(summary(dat$futime)), sd(dat$futime))
  futime_stats_nolc <- c(as.numeric(summary(dat$futime[dat$lc_index==0])), sd(dat$futime[dat$lc_index==0]))
  futime_stats_lc <- c(as.numeric(summary(dat$futime[dat$lc_index==1])), sd(dat$futime[dat$lc_index==1]))
  
  futime_stats <- data.frame(
    all = futime_stats_all,
    without_lc = futime_stats_nolc,
    with_lc = futime_stats_lc
  )
  
  rownames(futime_stats) <- c("min", "q1", "median", "mean", "q3", "max", "sd")
  
  write.csv(futime_stats, file=paste(out_dir, "/futime_stats_", outcome, "_all.csv", sep=""))
  
}
