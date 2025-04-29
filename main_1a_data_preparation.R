library(sparklyr)
library(tidyverse)
library(dbplyr)

cutoff_date = "2022-09-30"

############################### READ IN DATASETS ###############################

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

### read in CIS visit-level dataset
cis_visit <- spark_read_csv(sc, name="data_participant_clean", path="filepath/data_participant_clean_20230331.csv", 
                            header=TRUE, infer_schema=TRUE)

vars_of_interest <- c(
  "participant_id",
  "hh_id_fake",
  "visit_id",
  "visit_number",
  "visit_date",
  "dataset",
  "d_survey_mode_preference",
  "age_at_visit",
  "sex",
  "ethnicityg",
  "country",
  "gor9d",
  "cis20_samp",
  "imd_samp",
  "work_status_v1",
  "work_sector",
  "work_direct_contact_patients_etc",
  "gold_code",
  "health_conditions",
  "health_conditions_impact",
  "result_mk",
  "result_combined",
  "ct_mean",
  "ctpattern",
  "covid_test_swab_pos_first_date",
  "covid_test_blood_pos_first_date",
  "covid_date",
  "long_covid_have_symptoms",
  "reduce_activities_long_covid"
)

dat <- cis_visit %>%
  select(all_of(vars_of_interest))

### convert all dates to numeric
dat <- dat %>% 
  mutate(visit_date = to_date(visit_date, "ddMMMyyyy"),
         covid_date = to_date(covid_date, "ddMMMyyyy"),
         covid_test_swab_pos_first_date = to_date(covid_test_swab_pos_first_date, "ddMMMyyyy"),
         covid_test_blood_pos_first_date = to_date(covid_test_blood_pos_first_date, "ddMMMyyyy"))

### drop visits after cut-off date
dat <- dat %>%
  filter(visit_date <= cutoff_date)

### remove duplicate visits
dat <- dat %>%
  window_order(participant_id, visit_date, desc(visit_id)) %>%
  distinct(participant_id, visit_date, .keep_all=TRUE)

### read in vaccination data
cis_vacc <- spark_read_csv(sc, name="data_participant_vaccination", path="s3a://onscdp-prd-data01-d4946922/dapsen/landing_zone/ons/covid_19_infection_survey_fin/2023_04/v1/data_participant_vaccination_20230331.csv", 
                           header=TRUE, infer_schema=TRUE)

dat_vacc <- cis_vacc %>%
  select(participant_id, covid_vaccine_date1)

### convert all dates to numeric
dat_vacc <- dat_vacc %>% 
  mutate(covid_vaccine_date1 = to_date(covid_vaccine_date1, "ddMMMyyyy"))

### join vaccination data
dat <- dat %>%
  left_join(dat_vacc, by=join_by(participant_id==participant_id))

############################### EARLIEST DATES ###############################

### find date of earliest CIS visit
dat_visit0 <- dat %>%
  group_by(participant_id) %>%
  summarise(visit0_date = min(visit_date))

dat <- dat %>%
  left_join(dat_visit0, by=join_by(participant_id==participant_id))

### find date of latest CIS visit
dat_last_visit <- dat %>%
  group_by(participant_id) %>%
  summarise(last_visit_date = max(visit_date))

dat <- dat %>%
  left_join(dat_last_visit, by=join_by(participant_id==participant_id))

### find date of earliest positive PCR test during study
dat_swab_study <- dat %>%
  filter(result_mk==1) %>%
  group_by(participant_id) %>%
  summarise(swab_study_date = min(visit_date))

dat <- dat %>%
  left_join(dat_swab_study, by=join_by(participant_id==participant_id))

### find earliest positive blood test during the study
dat_blood_study <- dat %>%
  filter(result_combined==1) %>%
  group_by(participant_id) %>%
  summarise(blood_study_date = min(visit_date))

dat <- dat %>%
  left_join(dat_blood_study, by=join_by(participant_id==participant_id))

### find dates of earliest positive tests outside of study
dat_swab_blood_nonstudy <- dat %>%
  group_by(participant_id) %>%
  summarise(swab_nonstudy_date = min(covid_test_swab_pos_first_date),
            blood_nonstudy_date = min(covid_test_blood_pos_first_date))

dat <- dat %>%
  left_join(dat_swab_blood_nonstudy, by=join_by(participant_id==participant_id))

### find date when participants first thought they had COVID
### Note: restricting this to visits from 15 Oct 2020, as evidence that a large
### proportion of participants change their mind re. when they first thought
### they had COVID on follow-up visits after this (i.e. people thought they had
### COVID in the first wave, then during the second wave, when knowledge of the
### virus had improved, they amended their first COVID date to later, or got rid
### of it altogether)

dat_think_covid <- dat %>%
  filter(visit_date >= "2020-10-15") %>%
  group_by(participant_id) %>%
  summarise(think_covid_date = min(covid_date))

dat <- dat %>%
  left_join(dat_think_covid, by=join_by(participant_id==participant_id))

### set LC responses before 3 Feb 2021 (when the question was implemented) to NA
dat <- dat %>%
  mutate(long_covid_have_symptoms = ifelse(visit_date < "2021-02-03", NA, long_covid_have_symptoms))

### set positive antibody results after first vaccination to NA
dat <- dat %>%
  mutate(blood_study_date = ifelse(blood_study_date >= covid_vaccine_date1, NA, blood_study_date),
         blood_nonstudy_date = ifelse(blood_nonstudy_date >= covid_vaccine_date1, NA, blood_nonstudy_date))

### COVID-19 arrived in the UK on 24 Jan 2020 - set any infection dates before this to NA
dat <- dat %>%
  mutate(swab_nonstudy_date = ifelse(swab_nonstudy_date < "2020-01-24", NA, swab_nonstudy_date),
         blood_nonstudy_date = ifelse(blood_nonstudy_date < "2020-01-24", NA, blood_nonstudy_date),
         think_covid_date = ifelse(think_covid_date < "2020-01-24", NA, think_covid_date))

### find infection date based on any suspected or confirmed infection
dat <- dat %>%
  mutate(infection_date = pmin(swab_study_date, swab_nonstudy_date,
                               blood_study_date, blood_nonstudy_date,
                               think_covid_date, na.rm=TRUE))

### find date of first positive swab
dat <- dat %>%
  mutate(first_swab_date = pmin(swab_study_date, swab_nonstudy_date, na.rm=TRUE))

### set infection dates to NA if later than last visit during follow-up period
dat <- dat %>%
  mutate(infection_date = ifelse(infection_date > last_visit_date, NA, infection_date),
         first_swab_date = ifelse(first_swab_date > last_visit_date, NA, first_swab_date))

############################## LONG COVID STATUS ##############################

### derive flag for LC of any severity
dat <- dat %>%
  mutate(lc_any = ifelse(!is.na(long_covid_have_symptoms) &
                           long_covid_have_symptoms==1, 1, 0))

### set LC flag to 0 if before first positive swab or within first 12 weeks
dat <- dat %>%
  mutate(lc_any = ifelse(is.na(first_swab_date) |
                           (!is.na(first_swab_date) & datediff(visit_date, first_swab_date) < 84), 0, lc_any))

############################## SOCIO-DEMOGRAPHICS ##############################

### define 10-year age-band variable
dat <- dat %>%
  mutate(age10 = case_when(age_at_visit>=2 & age_at_visit<=15 ~ "02-15",
                           age_at_visit>=16 & age_at_visit<=24 ~ "16-24",
                           age_at_visit>=25 & age_at_visit<=34 ~ "25-34",
                           age_at_visit>=35 & age_at_visit<=49 ~ "35-49",
                           age_at_visit>=50 & age_at_visit<=64 ~ "50-64",
                           age_at_visit>=65 ~ "65+",
                           TRUE ~ "Missing"))

### define white/non-white variable
dat <- dat %>%
  mutate(white = ifelse(ethnicityg==1, 1, 0))

### define IMD quintile
dat <- dat %>%
  mutate(imd_quintile_eng = case_when(imd_samp>(0*32844/5) & imd_samp<=(1*32844/5) ~ 1,
                                      imd_samp>(1*32844/5) & imd_samp<=(2*32844/5) ~ 2,
                                      imd_samp>(2*32844/5) & imd_samp<=(3*32844/5) ~ 3,
                                      imd_samp>(3*32844/5) & imd_samp<=(4*32844/5) ~ 4,
                                      imd_samp>(4*32844/5) & imd_samp<=(5*32844/5) ~ 5,
                                      TRUE ~ -999),
         
         imd_quintile_wal = case_when(imd_samp>(0*1909/5) & imd_samp<=(1*1909/5) ~ 1,
                                      imd_samp>(1*1909/5) & imd_samp<=(2*1909/5) ~ 2,
                                      imd_samp>(2*1909/5) & imd_samp<=(3*1909/5) ~ 3,
                                      imd_samp>(3*1909/5) & imd_samp<=(4*1909/5) ~ 4,
                                      imd_samp>(4*1909/5) & imd_samp<=(5*1909/5) ~ 5,
                                      TRUE ~ -999),
         
         imd_quintile_sco = case_when(imd_samp>(0*6976/5) & imd_samp<=(1*6976/5) ~ 1,
                                      imd_samp>(1*6976/5) & imd_samp<=(2*6976/5) ~ 2,
                                      imd_samp>(2*6976/5) & imd_samp<=(3*6976/5) ~ 3,
                                      imd_samp>(3*6976/5) & imd_samp<=(4*6976/5) ~ 4,
                                      imd_samp>(4*6976/5) & imd_samp<=(5*6976/5) ~ 5,
                                      TRUE ~ -999),
         
         imd_quintile_ni = case_when(imd_samp>(0*890/5) & imd_samp<=(1*890/5) ~ 1,
                                     imd_samp>(1*890/5) & imd_samp<=(2*890/5) ~ 2,
                                     imd_samp>(2*890/5) & imd_samp<=(3*890/5) ~ 3,
                                     imd_samp>(3*890/5) & imd_samp<=(4*890/5) ~ 4,
                                     imd_samp>(4*890/5) & imd_samp<=(5*890/5) ~ 5,
                                     TRUE ~ -999),
         
         imd_quintile = case_when(country==0 ~ imd_quintile_eng,
                                  country==1 ~ imd_quintile_wal,
                                  country==3 ~ imd_quintile_sco,
                                  country==2 ~ imd_quintile_ni,
                                  TRUE ~ -999))

### impute missing health conditions and impact variables
dat <- dat %>%
  mutate(health_conditions = ifelse(is.na(health_conditions), 0, health_conditions),
         health_conditions_impact = ifelse(is.na(health_conditions_impact), 0, health_conditions_impact))

### define health/disability status variable
dat <- dat %>%
  mutate(health_status = case_when(health_conditions==1 & health_conditions_impact==0 ~ 1,
                                   health_conditions==1 & health_conditions_impact==1 ~ 2,
                                   health_conditions==1 & health_conditions_impact==2 ~ 3,
                                   TRUE ~ 0))

### rename work status
dat <- dat %>%
  rename("work_status" = "work_status_v1")

### set work sector to NA if not applicable, or work status is NA or 'not working'
dat <- dat %>%
  mutate(work_sector = ifelse(work_sector==99 |
                                is.na(work_status) | work_status %in% 5:12, NA, work_sector))

### derive SOC major group
dat <- dat %>%
  mutate(soc_major = substr(gold_code, 1, 1))

### set SOC major group to NA if SOC code is uncodeable, or work status is NA or 'not working'
dat <- dat %>%
  mutate(soc_major = ifelse(soc_major %in% c("u", "0") |
                              is.na(work_status) | work_status %in% 5:12, NA, soc_major),
         soc_major = as.numeric(soc_major))

### employment summary flags
dat <- dat %>%
  mutate(working = ifelse(work_status %in% 1:4, 1, 0),
         self_employed = ifelse(work_status %in% 3:4, 1, 0),
         offwork = ifelse(work_status %in% 2:4, 1, 0),
         nonworking= ifelse(work_status %in% 5:12, 1, 0),
         nonworking2 = ifelse(work_status %in% 5:6, 1, 0),
         unemployed = ifelse(work_status %in% 5, 1, 0),
         notlooking = ifelse(work_status %in% 6, 1, 0),
         retired = ifelse(work_status %in% 7, 1, 0),
         student = ifelse(work_status %in% 8:12, 1, 0),
         inactive = ifelse(work_status %in% 6:12, 1, 0),
         healthsocialcare_pf = ifelse(work_direct_contact_patients_etc==1 & work_sector %in% 2:3, 1, 0))

### derive data collection mode
dat <- dat %>%
  mutate(remote_collection = ifelse(dataset==3, 1, 0))

############################## FINALISE DATASET ##############################

### save dataset
spark_write_table(dat, name="filepath.lc_employment_survival_dataset1", format="parquet", mode="overwrite")
