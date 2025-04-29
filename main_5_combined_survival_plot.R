library(survminer)
library(patchwork)

infile = "filepath/plot_list_all_outcomes.RData"
infile_50plus = "filepath/plot_list_all_outcomes_50plus.RData"
out_dir = "filepath"

### load plot lists
load(infile)
load(infile_50plus)

### prepare plots for individual outcomes before combining
plot_change_sector <- plot_list_all_outcomes[[1]] +
  geom_text(label="(a)", x=70, y=0.18, size=4) +
  theme(
    axis.title.x=element_blank(),
    axis.text.x=element_text(size=8, colour="black", face="plain"),
    axis.ticks.x=element_line(size=0.5, colour="black"),
    axis.line.x=element_blank(),
    axis.title.y=element_text(size=9, colour="black", face="bold"),
    axis.text.y=element_text(size=8, colour="black", face="plain"),
    axis.ticks.y=element_line(size=0.5, colour="black"),
    axis.line.y=element_blank(),
    panel.border=element_rect(size=0.5, colour="black", fill=NA),
    panel.background=element_blank(),
    panel.grid.major=element_blank(),
    panel.grid.minor=element_blank(),
    strip.background=element_blank(),
    strip.text=element_text(size=10, colour="black", face="bold", hjust=0.5),
    legend.title=element_blank(),
    legend.position="bottom",
    legend.direction="horizontal",
    legend.justification="center",
    legend.text=element_text(size=9, colour="black", face="plain")
  )

plot_change_soc <- plot_list_all_outcomes[[2]] +
  geom_text(label="(b)", x=70, y=0.133, size=4) +
  theme(
    axis.title.x=element_blank(),
    axis.text.x=element_text(size=8, colour="black", face="plain"),
    axis.ticks.x=element_line(size=0.5, colour="black"),
    axis.line.x=element_blank(),
    axis.title.y=element_blank(),
    axis.text.y=element_text(size=8, colour="black", face="plain"),
    axis.ticks.y=element_line(size=0.5, colour="black"),
    axis.line.y=element_blank(),
    panel.border=element_rect(size=0.5, colour="black", fill=NA),
    panel.background=element_blank(),
    panel.grid.major=element_blank(),
    panel.grid.minor=element_blank(),
    strip.background=element_blank(),
    strip.text=element_text(size=10, colour="black", face="bold", hjust=0.5),
    legend.title=element_blank(),
    legend.position="bottom",
    legend.direction="horizontal",
    legend.justification="center",
    legend.text=element_text(size=9, colour="black", face="plain")
  )

plot_retirement <- plot_list_all_outcomes[[3]] +
  geom_text(label="(c)", x=70, y=0.0332, size=4) +
  theme(
    axis.title.x=element_text(size=9, colour="black", face="bold"),
    axis.text.x=element_text(size=8, colour="black", face="plain"),
    axis.ticks.x=element_line(size=0.5, colour="black"),
    axis.line.x=element_blank(),
    axis.title.y=element_text(size=9, colour="black", face="bold"),
    axis.text.y=element_text(size=8, colour="black", face="plain"),
    axis.ticks.y=element_line(size=0.5, colour="black"),
    axis.line.y=element_blank(),
    panel.border=element_rect(size=0.5, colour="black", fill=NA),
    panel.background=element_blank(),
    panel.grid.major=element_blank(),
    panel.grid.minor=element_blank(),
    strip.background=element_blank(),
    strip.text=element_text(size=10, colour="black", face="bold", hjust=0.5),
    legend.title=element_blank(),
    legend.position="bottom",
    legend.direction="horizontal",
    legend.justification="center",
    legend.text=element_text(size=9, colour="black", face="plain")
  )

plot_retirement_50plus <- plot_list_all_outcomes_50plus[[3]] +
  geom_text(label="(d)", x=70, y=0.093, size=4) +
  theme(
    axis.title.x=element_text(size=9, colour="black", face="bold"),
    axis.text.x=element_text(size=8, colour="black", face="plain"),
    axis.ticks.x=element_line(size=0.5, colour="black"),
    axis.line.x=element_blank(),
    axis.title.y=element_blank(),
    axis.text.y=element_text(size=8, colour="black", face="plain"),
    axis.ticks.y=element_line(size=0.5, colour="black"),
    axis.line.y=element_blank(),
    panel.border=element_rect(size=0.5, colour="black", fill=NA),
    panel.background=element_blank(),
    panel.grid.major=element_blank(),
    panel.grid.minor=element_blank(),
    strip.background=element_blank(),
    strip.text=element_text(size=10, colour="black", face="bold", hjust=0.5),
    legend.title=element_blank(),
    legend.position="bottom",
    legend.direction="horizontal",
    legend.justification="center",
    legend.text=element_text(size=9, colour="black", face="plain")
  )

### combine plots
plot_comb <- (plot_change_sector + plot_layout(guides="collect") & theme(legend.position="bottom")) +
  (plot_change_soc + theme(legend.position="none")) +
  (plot_retirement + theme(legend.position="none")) +
  (plot_retirement_50plus + theme(legend.position="none"))

ggsave(filename=paste0(out_dir, "/overlay_wtd.jpg"),
       plot=plot_comb,
       width=16, height=18, units="cm")
