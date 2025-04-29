########################### CHANGE IN SECTOR ###########################

### read in dataset
load("filepath/dataset_survival.RData")

### split out participants whose censor date was date of retirement
dat_sector <- dat[!is.na(dat$first_retired_date) &
                    dat$fudate_change_sector==dat$first_retired_date,]

### switch event date
dat_sector$first_change_sector_date <- dat_sector$first_retired_date
dat_sector$first_retired_date <- NA

### switch outcome
dat_sector$outcome_change_sector <- 3

### re-derive interval start time
dat_sector$t1_change_sector <- dat_sector$fudate_minus1_change_sector - dat_sector$index_date

### re-derive interval end time
dat_sector$t2_change_sector <- dat_sector$fudate_change_sector - dat_sector$index_date
dat_sector$t2_change_sector[dat_sector$t2_change_sector==0] <- 1

### replace rows on original dataset
dat <- dat[!(dat$participant_id %in% dat_sector$participant_id),]
dat <- rbind(dat, dat_sector)

### write out dataset
save(dat, file="filepath/dataset_survival_sector.RData")

########################### CHANGE IN SOC ###########################

### read in dataset
load("filepath/dataset_survival.RData")

### split out participants whose censor date was date of retirement
dat_soc <- dat[!is.na(dat$first_retired_date) &
                 dat$fudate_change_soc==dat$first_retired_date,]

### switch event date
dat_soc$first_change_soc_date <- dat_soc$first_retired_date
dat_soc$first_retired_date <- NA

### switch outcome
dat_soc$outcome_change_soc <- 3

### re-derive interval start time
dat_soc$t1_change_soc <- dat_soc$fudate_minus1_change_soc - dat_soc$index_date

### re-derive interval end time
dat_soc$t2_change_soc <- dat_soc$fudate_change_soc - dat_soc$index_date
dat_soc$t2_change_soc[dat_soc$t2_change_soc==0] <- 1

### replace rows on original dataset
dat <- dat[!(dat$participant_id %in% dat_soc$participant_id),]
dat <- rbind(dat, dat_soc)

### write out dataset
save(dat, file="filepath/dataset_survival_soc.RData")
