library(splines)
library(survival)
library(survminer)

infile = "filepath/dataset_survival_sector.RData"
out_dir = "filepath"
outcomes = c("change_sector")

############################# DATA PREP #############################

### read in dataset
load(infile)

### store original dataset
orig <- dat
rm(dat); gc()

### loop over outcome variables
for(i in 1:length(outcomes)) {
  
  ### reset dataset
  dat <- orig
  
  ### set up event and time variables
  outcome <- outcomes[i]
  outcome_var <- paste0("outcome_", outcome)
  t1_var <- paste0("t1_", outcome)
  t2_var <- paste0("t2_", outcome)
  
  ############################# STABALIZED IPWs #############################
  
  ### fit logistic model to derive SIPWs
  sipw_mod <- glm(
    lc_index ~
      ns(calendar_time_index, df=2, Boundary.knots=quantile(calendar_time_index, c(0.1,0.9))) +
      ns(age_index, df=2, Boundary.knots=quantile(age_index, c(0.1,0.9))) +
      sex_index +
      white_index +
      gor9d_index +
      imd_quintile_index +
      health_status_index +
      work_sector_index +
      soc_major_index +
      self_employed_index,
    family=binomial,
    data=dat
  )
  
  ### calculate marginal probabilities of each exposure group
  p1 <- sum(dat$lc_index) / nrow(dat)
  p0 <- 1 - p1
  
  ### extract predicted probabilities from model
  dat$pred1 <- predict(sipw_mod, type="response")
  dat$pred0 <- 1 - dat$pred1
  
  ### derive SIPWs
  dat$sipw1 <- p1 / dat$pred1
  dat$sipw0 <- p0 / dat$pred0
  dat$sipw <- ifelse(dat$lc_index==1, dat$sipw1, dat$sipw0)
  
  ### truncate SIPWs at 99th percentile and re-scale
  dat$sipw[dat$sipw > quantile(dat$sipw, 0.99)] <- quantile(dat$sipw, 0.99)
  dat$sipw <- dat$sipw * (nrow(dat) / sum(dat$sipw))
  
  ####################### UNWEIGHTED CUMULATIVE INCIDENCE #######################
  
  ### set up survival object
  surv_obj <- Surv(event = dat[[outcome_var]],
                   time = dat[[t1_var]],
                   time2 = dat[[t2_var]],
                   type = "interval")
  
  ### non-paramteric survival fit
  survfit_obj_unwtd <- survfit(surv_obj ~ lc_index, data=dat)
  
  ### plot cumulative incidence curves
  ggsurvplot(
    fit = survfit_obj_unwtd,
    data = dat,
    fun = function(x) 1-x,
    palette = c("#1b7837", "#762a83"),
    size = 0.5,
    font.x = 10,
    font.y = 10,
    font.tickslab = 10,
    font.legend = 10,
    conf.int = TRUE,
    censor = FALSE,
    axes.offset = FALSE,
    ylab = "Cumulative probability",
    legend = "bottom",
    legend.title = element_blank(),
    legend.labs = c("Without Long Covid", "With Long Covid")
  )
  
  ggsave(filename=paste0(out_dir, "/ci_unwtd_", outcome, "_all.jpg"),
         width=12, height=10, units="cm")
  
  ######################## WEIGHTED CUMULATIVE INCIDENCE ########################
  
  ### non-paramteric survival fit
  survfit_obj_wtd <- survfit(surv_obj ~ lc_index, data=dat, weights=sipw, robust=TRUE)
  
  ### plot cumulative incidence curves
  ggsurvplot(
    fit = survfit_obj_wtd,
    data = dat,
    fun = function(x) 1-x,
    palette = c("#1b7837", "#762a83"),
    size = 0.5,
    font.x = 10,
    font.y = 10,
    font.tickslab = 10,
    font.legend = 10,
    conf.int = TRUE,
    censor = FALSE,
    axes.offset = FALSE,
    ylab = "Cumulative probability",
    legend = "bottom",
    legend.title = element_blank(),
    legend.labs = c("Without Long Covid", "With Long Covid")
  )
  
  ggsave(filename=paste0(out_dir, "/ci_wtd_", outcome, "_all.jpg"),
         width=12, height=10, units="cm")
  
  ######################## SURVIVAL MODELLING ########################
  
  ### prepare intervals for modelling
  ### note: this is the 'interval2' approach when setting up a survival object
  ### (the 'interval' approach is not supported by parametric models, even though
  ### it gives identical results for the non-parametric approach when calling
  ### survfit); see the 'Details' section of ?Surv (paragraph starting "Interval
  ### censored data can be represented in two ways...")
  dat$t1_v2 <- dat[[t1_var]]
  dat$t2_v2 <- ifelse(dat[[outcome_var]]==0, Inf, dat[[t2_var]])
  
  ### for interval-censored follow-up time, if visit [t] = time 0 then add a
  ### constant (link function for all models is natural logarithm)
  dat$t1_v2[dat$t1_v2==0] <- 1
  
  ### set up survival object
  surv_obj_v2 <- Surv(time = dat[["t1_v2"]],
                      time2 = dat[["t2_v2"]],
                      type = "interval2")
  
  ### list baseline distributions to loop over
  dists <- c("weibull", "exponential", "lognormal", "loglogistic")
  dist_labels <- c("Weibull", "Exponential", "Log-normal", "Log-logistic")
  
  ### set up empty lists to store model outputs and data for plots
  mod_list <- as.list(NULL)
  plot_data_list <- as.list(NULL)
  
  for(j in 1:length(dists)) {
    
    ### set baseline distribution to fit
    dist_select <- dists[j]
    dist_name <- dist_labels[j]
    
    ### fit unweighted model
    mod_unwtd <- survreg(
      surv_obj_v2 ~ lc_index,
      dist=dist_select,
      data=dat
    )
    
    ### fit weighted model
    mod_wtd <- survreg(
      surv_obj_v2 ~ lc_index,
      dist=dist_select,
      data=dat,
      weights=sipw,
      robust=TRUE
    )
    
    ### extract and stack model outputs
    coeffs_unwtd <- as.data.frame(summary(mod_unwtd)$table)["lc_index",]
    rownames(coeffs_unwtd) <- NULL
    colnames(coeffs_unwtd) <- c("Coeff", "SE", "Z", "P")
    
    coeffs_wtd <- as.data.frame(summary(mod_wtd)$table)["lc_index",-3]
    rownames(coeffs_wtd) <- NULL
    colnames(coeffs_wtd) <- c("Coeff", "SE", "Z", "P")
    
    coeffs <- rbind(coeffs_unwtd, coeffs_wtd)
    
    coeffs$Distribution <- dist_name
    coeffs$Model <- c("Unweighted", "Weighted")
    coeffs$Exp_coeff <- exp(coeffs$Coeff)
    coeffs$LCL <- exp(coeffs$Coeff - 1.96 * coeffs$SE)
    coeffs$UCL <- exp(coeffs$Coeff + 1.96 * coeffs$SE)
    coeffs$AIC <- c(AIC(mod_unwtd), AIC(mod_wtd))
    coeffs$BIC <- c(BIC(mod_unwtd), BIC(mod_wtd))
    coeffs <- coeffs[,c(5:6, 1:4, 7:11)]
    
    ### store model outputs
    mod_list[[j]] <- coeffs
    
    ### set vector of probabilities at which to evaluate CDF (i.e. CI[t]) for plot
    probs_eval <- seq(0, 0.9999, 0.0001)
    
    ### generate predicted times from model - non-LC group
    preds0 <- predict(mod_wtd,
                      newdata=data.frame(lc_index=0),
                      type="quantile",
                      p=probs_eval,
                      se.fit=TRUE)
    
    ### generate predicted times from model - LC group
    preds1 <- predict(mod_wtd,
                      newdata=data.frame(lc_index=1),
                      type="quantile",
                      p=probs_eval,
                      se.fit=TRUE)
    
    ### extract point estimates
    preds0_est <- preds0[[1]]
    preds1_est <- preds1[[1]]
    
    ### calculate prediction intervals
    preds0_lcl <- preds0[[1]] - 1.96 * preds0[[2]]
    preds0_ucl <- preds0[[1]] + 1.96 * preds0[[2]]
    
    preds1_lcl <- preds1[[1]] - 1.96 * preds1[[2]]
    preds1_ucl <- preds1[[1]] + 1.96 * preds1[[2]]
    
    ### remove predictions outside of observed time range - non-LC group
    min_t0 <- min(dat[[t1_var]][dat$lc_index==0])
    max_t0 <- max(dat[[t2_var]][dat$lc_index==0])
    
    probs0 <- probs_eval[preds0[[1]]>=min_t0 & preds0[[1]]<=max_t0]
    preds0_est <- preds0_est[preds0[[1]]>=min_t0 & preds0[[1]]<=max_t0]
    preds0_lcl <- preds0_lcl[preds0[[1]]>=min_t0 & preds0[[1]]<=max_t0]
    preds0_ucl <- preds0_ucl[preds0[[1]]>=min_t0 & preds0[[1]]<=max_t0]
    
    ### remove predictions outside of observed time range - LC group
    min_t1 <- min(dat[[t1_var]][dat$lc_index==1])
    max_t1 <- max(dat[[t2_var]][dat$lc_index==1])
    
    probs1 <- probs_eval[preds1[[1]]>=min_t1 & preds1[[1]]<=max_t1]
    preds1_est <- preds1_est[preds1[[1]]>=min_t1 & preds1[[1]]<=max_t1]
    preds1_lcl <- preds1_lcl[preds1[[1]]>=min_t1 & preds1[[1]]<=max_t1]
    preds1_ucl <- preds1_ucl[preds1[[1]]>=min_t1 & preds1[[1]]<=max_t1]
    
    ### concatenate probability and time vectors
    probs <- c(probs0, probs1)
    preds_est <- c(preds0_est, preds1_est)
    preds_lcl <- c(preds0_lcl, preds1_lcl)
    preds_ucl <- c(preds0_ucl, preds1_ucl)
    
    ### collate dataset for plot
    plot_data <- data.frame(
      Distribution = rep(dist_name, length(preds_est)),
      Exposure = c(rep("Without Long Covid", length(preds0_est)),
                   rep("With Long Covid", length(preds1_est))),
      Time = preds_est,
      LCL = preds_lcl,
      UCL = preds_ucl,
      Cum_prob = probs
    )
    
    ### store dataset for plot
    plot_data_list[[j]] <- plot_data
    
  }
  
  ### collate model outputs and datasets for plots across distributions
  mod_out <- mod_list[[1]]
  plot_out <- plot_data_list[[1]]
  if(length(dists)>1) {
    for(j in 2:length(dists)) {
      mod_out <- rbind(mod_out, mod_list[[j]])
      plot_out <- rbind(plot_out, plot_data_list[[j]])
    }
  }
  
  ### save model outputs to working directory
  write.csv(mod_out,
            file=paste0(out_dir, "/model_output_", outcome, "_all.csv"),
            row.names=FALSE)
  
  ### plot non-parametric cumulative incidence curves
  nplme_plot <- ggsurvplot(
    fit = survfit_obj_wtd,
    data = dat,
    conf.int = TRUE,
    conf.int.style = "ribbon",
    fun = function(x) 1-x,
    palette = c("#1b7837", "#762a83"),
    linetype = 2,
    size = 0.3,
    font.x = 10,
    font.y = 10,
    font.tickslab = 10,
    font.legend = 10,
    censor = FALSE,
    axes.offset = FALSE,
    ylab = "Cumulative probability",
    legend = "bottom",
    legend.title = element_blank(),
    legend.labs = c("Without Long Covid", "With Long Covid")
  )
  
  ### add fitted line from each parametric model and add plots to list
  plot_list <- as.list(NULL)
  
  for(j in 1:length(dists)) {
    
    plot_list[[j]] <- nplme_plot$plot +
      geom_line(data=plot_out[plot_out$Distribution==dist_labels[j],],
                aes(x=Time, y=Cum_prob, group=Exposure, colour=Exposure),
                size=0.5) +
      labs(title=dist_labels[j]) +
      theme(
        plot.title=element_text(face="bold", size=11, hjust=0.5),
        plot.margin=margin(2,10,6,2)
      )
    
  }
  
  ### prepare plots for individual distributions before combining
  ### note: this isn't functionalised - if the number of baseline distributions
  ### changes then this bit of code needs to be updated manually
  plot_list[[1]] <- plot_list[[1]] +
    theme(
      axis.title.x=element_blank()
    )
  
  plot_list[[2]] <- plot_list[[2]] +
    theme(
      axis.title.x=element_blank(),
      axis.title.y=element_blank()
    )
  
  plot_list[[3]] <- plot_list[[3]]
  
  plot_list[[4]] <- plot_list[[4]] +
    theme(
      axis.title.y=element_blank()
    )
  
  ### combine plots
  ### note: this isn't functionalised - if the number of baseline distributions
  ### changes then this bit of code needs to be updated manually
  comb_plot <- ggarrange(plot_list[[1]], plot_list[[2]], plot_list[[3]], plot_list[[4]],
                         nrow=2,
                         ncol=2,
                         common.legend=TRUE,
                         legend="bottom")
  
  ggsave(filename=paste0(out_dir, "/parametric_fits_", outcome, "_all.jpg"),
         plot=comb_plot,
         width=16, height=18, units="cm")
  
  ### plot of best fitting parametric curve overlayed on non-parametric estimate
  mod_bic <- mod_out[mod_out$Model=="Weighted", c("Distribution", "BIC")]
  dist_min_bic <- mod_bic[mod_bic$BIC==min(mod_bic$BIC), "Distribution"]
  
  overlay_plot <- nplme_plot$plot +
    geom_line(data=plot_out[plot_out$Distribution==dist_min_bic,],
              aes(x=Time, y=Cum_prob, group=Exposure, colour=Exposure),
              size=0.5) +
    theme(
      plot.title=element_text(face="bold", size=11, hjust=0.5),
      plot.margin=margin(2,10,6,2)
    )
  
  ggsave(filename=paste0(out_dir, "/overlay_wtd_", outcome, "_all.jpg"),
         plot=print(overlay_plot),
         width=12, height=10, units="cm")
  
}
