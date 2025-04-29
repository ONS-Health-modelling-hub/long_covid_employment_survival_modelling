**Inputs:**
- data_participant_clean_20230331.csv
- data_participant_vaccination_20230331.csv

**Main analysis:**
- Run scripts prefixed with "main_" in alphanumerical order
- "cov_dist_cat.r" and "cov_dist_cont.r" are utility functions called by "main_2_descriptive_stats.r" and therefore do not need to be run on their own

**Sensitivity analyses (determined by prefix):**
- sa1: censor follow-up at first positive LC response after index date
- sa2: remove non-exposed participants who report LC after their index date
- sa3: control for health at enrolment rather tha at index date
- sa4: changes-in-employment-sector and change-in-occupation-group outcome statuses switched from censor to event for participants whose follow-up was right-censored at retirement
