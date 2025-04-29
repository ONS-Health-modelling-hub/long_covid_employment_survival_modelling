library(sparklyr)
library(tidyverse)
library(dbplyr)
library(zoo)
library(sqldf)

############################### INITIAL DATA PREP ###############################

### set up the Spark connection
xl_config <- spark_config()
xl_config$spark.executor.memory <- "20g"
xl_config$spark.yarn.executor.memoryOverhead <- "2g"
xl_config$spark.executor.cores <- 5
xl_config$spark.dynamicAllocation.enabled <- "true"
xl_config$spark.dynamicAllocation.maxExecutors <- 12
xl_config$spark.sql.shuffle.partitions <- 240
#xl_config$spark.shuffle.service.enabled <- "true"
xl_config$spark.sql.legacy.timeParserPolicy = "LEGACY"
#xl_config$spark.sql.session.timeZone = "GMT"
xl_config$spark.sql.session.timeZone = "UTC+01:00"
#xl_config$spark.sql.parquet.datetimeRebaseModeInRead = "CORRECTED"
xl_config$spark.sql.parquet.int96RebaseModeInRead = "CORRECTED"
xl_config$spark.sql.analyzer.maxIterations = 1000

sc <- spark_connect(master = "yarn-client",
                    app_name = "ONS_session",
                    config = xl_config)

### read in dataset
dat <- sdf_sql(sc, "SELECT * FROM filepath.lc_employment_survival_dataset1")

### find index date: first visit where LC question was answered within 12-20
### weeks of first positive swab
index_dates <- dat %>%
  filter(!is.na(long_covid_have_symptoms) &
         datediff(visit_date, first_swab_date) >= 84 &
         datediff(visit_date, first_swab_date) < 140) %>%
  group_by(participant_id) %>%
  summarise(index_date = min(visit_date))

dat <- dat %>%
  left_join(index_dates, by=join_by(participant_id==participant_id))

### get person-level non-health variables at index date
dat_index_nonhealth <- dat %>%
  filter(visit_date==index_date) %>%
  select(participant_id, long_covid_have_symptoms, age_at_visit, age10, sex, ethnicityg,
         white, imd_quintile, gor9d, country, work_status, work_sector, soc_major,
         self_employed, healthsocialcare_pf, remote_collection) %>%
  rename(lc_index = long_covid_have_symptoms, age_index = age_at_visit, age10_index = age10,
         sex_index = sex, ethnicityg_index = ethnicityg, white_index = white,
         imd_quintile_index = imd_quintile, gor9d_index = gor9d, country_index = country,
         work_status_index = work_status, work_sector_index = work_sector,
         soc_major_index = soc_major, self_employed_index = self_employed,
         healthsocialcare_pf_index = healthsocialcare_pf,
         remote_collection_index = remote_collection)

dat <- dat %>%
  left_join(dat_index_nonhealth, by=join_by(participant_id==participant_id))

### get person-level health variables at enrolment date
dat_index_health <- dat %>%
  filter(visit_date==visit0_date) %>%
  select(participant_id, health_conditions, health_conditions_impact, health_status) %>%
  rename(health_conditions_index = health_conditions,
         health_conditions_impact_index = health_conditions_impact,
         health_status_index = health_status)

dat <- dat %>%
  left_join(dat_index_health, by=join_by(participant_id==participant_id))

########################### DATASET FILTERING ###########################

### initial sample waterfall
n_people <- c(sdf_nrow(dat %>% distinct(participant_id)))
names(n_people) <- "Initial sample"

### drop participants without a positive swab, and whose first positive swab was
### before 11/11/2020 (12 weeks before LC question was introduced on 03/02/2021)
dat <- dat %>%
  filter(!is.na(first_swab_date) & first_swab_date >= "2020-11-11")

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Positive swab during study period (from 12 weeks before LC question was introduced on 03/02/2021)"

### drop participants whose first positive swab was on or before CIS enrolment
dat <- dat %>%
  filter(datediff(first_swab_date, visit0_date) > 0)

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "First positive swab after CIS enrolment visit"

### drop participants with confirmed or suspected infection more than 14 days
### before first positive swab
dat <- dat %>%
  filter(!((!is.na(infection_date) & is.na(first_swab_date)) |
               (!is.na(infection_date) &  datediff(first_swab_date, infection_date) > 14)))

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Without confirmed or suspected infection >14 days before first positive swab"

### keep participants who responded to LC question within 12-20 weeks of first
### positive swab
dat <- dat %>%
  filter(!is.na(index_date))

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Responded to LC question within 12-20 weeks of first positive swab"

### drop visits before index date
dat <- dat %>%
  filter(datediff(visit_date, index_date) >= 0)

### find number of visits per participant (index + post-index visits)
dat_n_visits <- dat %>%
  group_by(participant_id) %>%
  summarise(n_visits = n())

dat <- dat %>%
  left_join(dat_n_visits, by=join_by(participant_id==participant_id))

### keep participants with at least one post-index follow-up visit
dat <- dat %>%
  filter(n_visits > 1)

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "At least one post-index follow-up visit"

### restrict dataset to participants aged 16-64 at index date
dat <- dat %>%
  filter(age_index>=16 & age_index<=64)

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Aged 16-64 at index date"

### restrict dataset to participants in employment at index date
dat <- dat %>%
  filter(work_status_index %in% 1:4)

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Employed at index date"

### restrict dataset to participants with always complete work sector and occupation info
dat_ever_missing_info <- dat %>%
  mutate(missing_sector = ifelse(working==1 & is.na(work_sector), 1, 0),
         missing_soc = ifelse(working==1 & is.na(soc_major), 1, 0)) %>%
  group_by(participant_id) %>%
  summarise(ever_missing_sector = max(missing_sector),
            ever_missing_soc = max(missing_soc))

dat <- dat %>%
  left_join(dat_ever_missing_info, by=join_by(participant_id==participant_id)) %>%
  filter(ever_missing_sector==0 & ever_missing_soc==0)

n_people <- c(n_people, sdf_nrow(dat %>% distinct(participant_id)))
names(n_people)[length(n_people)] <- "Complete work sector and occupation info"

### write out sample waterfall
sample_waterfall <- as.data.frame(n_people)
colnames(sample_waterfall) <- "Participants"
write.csv(sample_waterfall, file="filepath/sample_waterfall.csv")

### restrict dataset to variables of interest
keepvars <- c(
  "participant_id",
  "visit_date",
  "age_at_visit",
  "lc_any",
  "work_status",
  "work_sector",
  "soc_major",
  "working",
  "self_employed",
  "offwork",
  "nonworking",
  "nonworking2",
  "unemployed",
  "notlooking",
  "retired",
  "student",
  "inactive",
  "remote_collection",
  "index_date",
  "lc_index",
  "age_index",
  "age10_index",
  "sex_index",
  "ethnicityg_index",
  "white_index",
  "imd_quintile_index",
  "gor9d_index",
  "country_index",
  "health_conditions_index",
  "health_conditions_impact_index",
  "health_status_index",
  "work_status_index",
  "work_sector_index",
  "soc_major_index",
  "self_employed_index",
  "remote_collection_index"
)

dat <- dat %>%
  select(all_of(keepvars))

### read dataset into memory
dat <- collect(dat)

########################### DERIVATIONS ###########################

### coerce categorical variables to factors
dat$work_status <- as.factor(dat$work_status)
dat$work_sector <- as.factor(dat$work_sector)
dat$soc_major <- as.factor(dat$soc_major)
dat$age10_index <- as.factor(dat$age10_index)
dat$ethnicityg_index <- as.factor(dat$ethnicityg_index)
dat$imd_quintile_index <- as.factor(dat$imd_quintile_index)
dat$gor9d_index <- as.factor(dat$gor9d_index)
dat$country_index <- as.factor(dat$country_index)
dat$health_conditions_impact_index <- as.factor(dat$health_conditions_impact_index)
dat$health_status_index <- as.factor(dat$health_status_index)
dat$work_status_index <- as.factor(dat$work_status_index)
dat$work_sector_index <- as.factor(dat$work_sector_index)
dat$soc_major_index <- as.factor(dat$soc_major_index)

### coerce dates to numeric
dat$visit_date <- as.numeric(dat$visit_date)
dat$index_date <- as.numeric(dat$index_date)

### derive calendar time of index date (days since 24 Jan 2020)
dat$calendar_time_index <- dat$index_date - as.numeric(as.Date("2020-01-24"))

### sort dataset by participant ID and visit date
dat <- dat[with(dat, order(participant_id, visit_date)), ]

### impute missing work sector and SOC group using LOCF (i.e. when participants weren't working,
### assume their sector and occupation continued from previously)
na.locf2 <- function(x) {na.locf(x, na.rm=FALSE)}
dat$work_sector <- ave(dat$work_sector, dat$participant_id, FUN=na.locf2)
dat$soc_major <- ave(dat$soc_major, dat$participant_id, FUN=na.locf2)

### flag changes in work sector and SOC group between successive follow-up visits
dat$prev_participant_id <- c("X", dat$participant_id[1:(nrow(dat)-1)])
dat$prev_work_sector <- c("X", dat$work_sector[1:(nrow(dat)-1)])
dat$prev_soc_major <- c("X", dat$soc_major[1:(nrow(dat)-1)])

dat$change_sector <- ifelse(dat$participant_id==dat$prev_participant_id &
                              dat$work_sector!=dat$prev_work_sector, 1, 0)

dat$change_soc <- ifelse(dat$participant_id==dat$prev_participant_id &
                           dat$soc_major!=dat$prev_soc_major, 1, 0)

### find date of first visit when changed work sector
dat <- sqldf("
  select a.*, b.first_change_sector_date
  from dat as a
  left join(
    select participant_id, min(visit_date) as first_change_sector_date
    from dat
    where change_sector = 1
    group by participant_id
  ) as b
  on a.participant_id = b.participant_id
")

### find date of first visit when changed SOC group
dat <- sqldf("
  select a.*, b.first_change_soc_date
  from dat as a
  left join(
    select participant_id, min(visit_date) as first_change_soc_date
    from dat
    where change_soc = 1
    group by participant_id
  ) as b
  on a.participant_id = b.participant_id
")

### find date of first visit when retired
dat <- sqldf("
  select a.*, b.first_retired_date
  from dat as a
  left join(
    select participant_id, min(visit_date) as first_retired_date
    from dat
    where retired = 1
    group by participant_id
  ) as b
  on a.participant_id = b.participant_id
")

### find date of first visit when student
dat <- sqldf("
  select a.*, b.first_student_date
  from dat as a
  left join(
    select participant_id, min(visit_date) as first_student_date
    from dat
    where student = 1
    group by participant_id
  ) as b
  on a.participant_id = b.participant_id
")

### find date of first visit when aged 65+
dat <- sqldf("
  select a.*, b.first_age65_date
  from dat as a
  left join(
    select participant_id, min(visit_date) as first_age65_date
    from dat
    where age_at_visit >= 65
    group by participant_id
  ) as b
  on a.participant_id = b.participant_id
")

### find date of last visit during study period
dat$last_visit_date <- ave(dat$visit_date, dat$participant_id, FUN=max)

### find date of penultimate visit during study period
second.max <- function(x) {return(max(x[x!=max(x)]))}
dat$penultimate_visit_date <- ave(dat$visit_date, dat$participant_id, FUN=second.max)

### derive derive end-of-follow-up date for each outcome: either date of first event, or date
### of censor (i.e. when no longer in risk set for experiencing event)

### change in sector: censor at retirement, becoming a student, turning 65, or last follow-up visit
dat$fudate_change_sector <- pmin(dat$first_change_sector_date,
                                 dat$first_retired_date,
                                 dat$first_student_date,
                                 dat$first_age65_date,
                                 dat$last_visit_date,
                                 na.rm=TRUE)

### change in SOC group: censor at retirement, becoming a student, turning 65, or last follow-up visit
dat$fudate_change_soc <- pmin(dat$first_change_soc_date,
                              dat$first_retired_date,
                              dat$first_student_date,
                              dat$first_age65_date,
                              dat$last_visit_date,
                              na.rm=TRUE)

### retirement: censor at becoming a student, turning 65, or last follow-up visit
dat$fudate_retired <- pmin(dat$first_retired_date,
                           dat$first_student_date,
                           dat$first_age65_date,
                           dat$last_visit_date,
                           na.rm=TRUE)

### find date of the visit preceding the end-of-follow-up date for each outcome; this is
### required because censoring events have already happened by the time they are first observed
### at visit [t], so visit [t-1] is the last time participants are still in the risk set
dat <- sqldf("
  select
    a.*,
    b.fudate_minus1_change_sector,
    c.fudate_minus1_change_soc,
    d.fudate_minus1_retired

  from dat as a
  
  left join(
    select participant_id, max(visit_date) as fudate_minus1_change_sector
    from dat
    where visit_date < fudate_change_sector
    group by participant_id
  ) as b
  on a.participant_id=b.participant_id
  
  left join(
    select participant_id, max(visit_date) as fudate_minus1_change_soc
    from dat
    where visit_date < fudate_change_soc
    group by participant_id
  ) as c
  on a.participant_id=c.participant_id
  
  left join(
    select participant_id, max(visit_date) as fudate_minus1_retired
    from dat
    where visit_date < fudate_retired
    group by participant_id
  ) as d
  on a.participant_id=d.participant_id
")

### derive event/censor flag for each outcome
### note: for datasets with right-censoring (the usual set up), the outcome flag is usually 1
### (event) or 0 (censor); here we have interval censoring (i.e. events take place between visits
### [t-1] and [t]), so an event is denoted as 3 instead of 1 (see 'event' argument in ?Surv)

dat$outcome_change_sector <- ifelse(dat$fudate_change_sector==dat$first_change_sector_date &
                                      !is.na(dat$first_change_sector_date), 3, 0)

dat$outcome_change_soc <- ifelse(dat$fudate_change_soc==dat$first_change_soc_date &
                                   !is.na(dat$first_change_soc_date), 3, 0)

dat$outcome_retired <- ifelse(dat$fudate_retired==dat$first_retired_date &
                                !is.na(dat$first_retired_date), 3, 0)

### derive interval start times for each outcome:
### for events, FU time is interval-censored starting at visit [t-1]
### for censors before last visit date, FU time is right-censored at visit [t-1]
### for censors at last visit date, FU time is right-censored at visit [t]

dat$t1_change_sector <- ifelse(dat$outcome_change_sector==3,
                               dat$fudate_minus1_change_sector - dat$index_date,
                               ifelse(dat$fudate_change_sector==dat$last_visit_date,
                                      dat$fudate_change_sector - dat$index_date,
                                      dat$fudate_minus1_change_sector - dat$index_date))

dat$t1_change_soc <- ifelse(dat$outcome_change_soc==3,
                            dat$fudate_minus1_change_soc - dat$index_date,
                            ifelse(dat$fudate_change_soc==dat$last_visit_date,
                                   dat$fudate_change_soc - dat$index_date,
                                   dat$fudate_minus1_change_soc - dat$index_date))

dat$t1_retired <- ifelse(dat$outcome_retired==3,
                         dat$fudate_minus1_retired - dat$index_date,
                         ifelse(dat$fudate_retired==dat$last_visit_date,
                                dat$fudate_retired - dat$index_date,
                                dat$fudate_minus1_retired - dat$index_date))

### derive interval end times for each outcome:
### for events, FU time is interval-censored ending at visit [t]
### for censors before last visit date, FU time is right-censored at visit [t-1]
### for censors at last visit date, FU time is right-censored at visit [t]

dat$t2_change_sector <- ifelse(dat$outcome_change_sector==3,
                               dat$fudate_change_sector - dat$index_date,
                               dat$t1_change_sector)

dat$t2_change_soc <- ifelse(dat$outcome_change_soc==3,
                            dat$fudate_change_soc - dat$index_date,
                            dat$t1_change_soc)

dat$t2_retired <- ifelse(dat$outcome_retired==3,
                         dat$fudate_retired - dat$index_date,
                         dat$t1_retired)

### set interval end time to 1 if 0 (otherwise not in the risk set to begin with)
dat$t2_change_sector[dat$t2_change_sector==0] <- 1
dat$t2_change_soc[dat$t2_change_soc==0] <- 1
dat$t2_retired[dat$t2_retired==0] <- 1

### find work sector before and after change-in-sector event
dat <- sqldf("
  select a.*, b.work_sector_t1, b.work_sector_t2
  from dat as a
  left join(
    select
      participant_id,
      work_sector as work_sector_t1,
      prev_work_sector as work_sector_t2
    from dat
    where outcome_change_sector=3 and visit_date=first_change_sector_date
  ) as b
  on a.participant_id=b.participant_id
")

### find SOC group before and after change-in-SOC event
dat <- sqldf("
  select a.*, b.soc_major_t1, b.soc_major_t2
  from dat as a
  left join(
    select
      participant_id,
      soc_major as soc_major_t1,
      prev_soc_major as soc_major_t2
    from dat
    where outcome_change_soc=3 and visit_date=first_change_soc_date
  ) as b
  on a.participant_id=b.participant_id
")

### collapse to person-level dataset
dat <- dat[!duplicated(dat$participant_id),]

### write out dataset
save(dat, file="filepath/dataset_survival.RData")
