### Task2
#Create plots that show the frequency of Reddit comments over the entire time period 
#(aggregate to an hourly level). Also create plots that show the frequency of comments 
#over the days of the week (data should again be at an hour level). 
#Comment on any patterns you notice, particularly days with unusually large or small numbers
#of comments.

#Recreate the above plots but only for comments that were gilded 
#(commentor was given Reddit gold by another user). 
#Comment on the similarly or difference in the plot collections.

###########################################################################
########################### Prepare the SPARK #############################
###########################################################################

#Large dataset 
Sys.setenv(HADOOP_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(YARN_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(SPARK_HOME="/data/hadoop/spark")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R/lib"), .libPaths()))
library(SparkR)
sc = sparkR.init()
sqlContext = sparkRSQL.init(sc)

#Try code with small dataset 
j = jsonFile(sqlContext, "hdfs://localhost:9000/data/RC_2015-02.json")
registerTempTable(j, "reddit")
reddit = SparkR::sql(sqlContext, "SELECT created_utc, gilded FROM reddit")
#Unix coding time transformation
reddit$time=from_unixtime(reddit$created_utc, format = "yyyy-MM-dd HH:mm:ss")
reddit$month=month(reddit$time)
reddit$date=dayofmonth(reddit$time)
reddit$hour=hour(reddit$time)

###################################################################################
################### DataFrame Collect EXTRACTION FOR GILD == 0&1 ##################
###################################################################################
#### group by "month", "date", "hour" (this is for the second plot)
#res_long_group=count(group_by(reddit, "time", "date", "hour"))
#collect dataset and use for later weekday transformation
res_long_group=count(group_by(reddit, "time"))
res_long_all = SparkR::collect(res_long_group)

# group by hour (this is for the first plot)
# res_long_groupbyhour=count(group_by(reddit,"hour", "date"))
# res_long_groupbyhour=SparkR::collect(res_long_groupbyhour)

# check the size
# dim(res_long_all) #  2418648       2
# dim(res_long_groupbyhour) # 672   3

###################################################################################
################### DataFrame Collect EXTRACTION FOR GILD == 1 ####################
###################################################################################
#Selecting subset of gilded comments
res_short_group = count(group_by(reddit[reddit$gilded==1,], "time"))
res_short = SparkR::collect(res_short_group)

# group by hour (this is for the first plot)
# res_short_groupbyhour = count(group_by(reddit[reddit$gilded==1,],"hour", "date"))
# res_short_groupbyhour = SparkR::collect(res_short_groupbyhour)

dim(res_short) # 16236     2
# dim(res_short_groupbyhour) # 739   3

###################################################################################
################### DataFrame Extract EXTRACTION FOR GILD == 1&0 ##################
###################################################################################

# full dataframe for full data set
res_long_all = res_long_all[which(as.POSIXlt(res_long_all$time)$mon == 1),]
res_long_all$month = as.POSIXlt(res_long_all$time)$mon + 1
res_long_all$hour = as.POSIXlt(res_long_all$time)$hour
res_long_all$date = as.POSIXlt(res_long_all$time)$mday


# grouped dataframe for both gilted and not gilted
#detach("package:plyr", unload=TRUE) 
library(dplyr)
byMon <- group_by(res_long_all, hour, date)
sumMon_res_long_all <- summarize(byMon, totalCount = sum(count)) 

###################################################################################
################### DataFrame Extract EXTRACTION FOR GILD == 1 ####################
###################################################################################

# full dataframe for full data set
res_short = res_long_all[which(as.POSIXlt(res_short$time)$mon == 1),]
res_short$month = as.POSIXlt(res_short$time)$mon + 1
res_short$hour = as.POSIXlt(res_short$time)$hour
res_short$date = as.POSIXlt(res_short$time)$mday

# grouped dataframe for both gilted and not gilted
#detach("package:plyr", unload=TRUE) 
library(dplyr)
byMon_res_short <- group_by(res_short, hour, date)
sumMon_res_short <- summarize(byMon_res_short, totalCount = sum(count))

#############################################################################################
############################# PLOT AND ANALYSIS #############################################
#############################################################################################

########################## FIRST PLOT FOR GILD == 0 & 1  ############################
library(dplyr)

mixgrid_plot1 = sumMon_res_long_all[order(sumMon_res_long_all$date, sumMon_res_long_all$hour),]
dim(mixgrid_plot1) #667   3

# plot the first plot
par(mfrow=c(1,1))
plot(x=seq(1, dim(mixgrid_plot1)[1], by = 1), y = mixgrid_plot1$totalCount, type = "l", 
     xlab = "Hour", ylab = "Frequency", main = "Full Data Feburary 1-28 Hourly Frequency")

########################## SECOND PLOT FOR GILD == 0 & 1  ############################
res_long_all$weekday=weekdays(as.POSIXlt(res_long_all$time))
test<- group_by(res_long_all, hour, weekday)
testsum <- summarize(test,countWeekday=sum(count))

# Rule the order of the weekday names
testsum$weekday <- factor(testsum$weekday, levels= c("Monday","Tuesday", 
                                                     "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
testsum_new = testsum[order(testsum$weekday, testsum$hour),]
testsum_new$mark = paste(testsum_new$weekday, " Hour: ",testsum_new$hour, sep = "")

par(mfrow=c(4,2))
plot(seq(1, 24, by = 1), testsum_new$countWeekday[1:24], type = "l", main = "Full Data Aggregated Monday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[25:48], type = "l", main = "Full Data Aggregated Tuesday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[49:72], type = "l", main = "Full Data Aggregated Wednesday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[73:96], type = "l", main = "Full Data Aggregated Thurseday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[97:120], type = "l", main = "Full Data Aggregated Friday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[121:144], type = "l", main = "Full Data Aggregated Saturday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new$countWeekday[145:168], type = "l", main = "Full Data Aggregated Sunday", ylab = "Frequency")

########################## FIRST PLOT FOR GILD ==  1  ###################################

hour_month_plot_gild = sumMon_res_short[order(sumMon_res_short$date, sumMon_res_short$hour),]
dim(hour_month_plot_gild) # 669    3

# plot the first plot
par(mfrow=c(1,1))
plot(x=seq(1, dim(hour_month_plot_gild)[1], by = 1), y = hour_month_plot_gild$totalCount, type = "l", 
     xlab = "Hour", ylab = "Frequency", main = "Gilded Feburary 1-28 Hourly Frequency")

########################## SECOND PLOT FOR GILD == 1  ############################

res_short$weekday=weekdays(as.POSIXlt(res_short$time))
test_1<- group_by(res_short,hour,weekday)
testsum_1 <- summarize(test_1, countWeekday=sum(count))

# Rule the order of the weekday names
testsum_1$weekday <- factor(testsum_1$weekday, levels= c("Monday","Tuesday", 
                                                         "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
testsum_new_1 = testsum_1[order(testsum_1$weekday, testsum_1$hour),]
testsum_new_1$mark = paste(testsum_new_1$weekday, " Hour: ",testsum_new_1$hour, sep = "")

par(mfrow=c(4,2))
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[1:24], type = "l", main = "Gilded Aggregated Monday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[25:48], type = "l", main = "Gilded Aggregated Tuesday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[49:72], type = "l", main = "Gilded Aggregated Wednesday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[73:96], type = "l", main = "Gilded Aggregated Thurseday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[97:120], type = "l", main = "Gilded Aggregated Friday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[121:144], type = "l", main = "Gilded Aggregated Saturday", ylab = "Frequency")
plot(seq(1, 24, by = 1), testsum_new_1$countWeekday[145:168], type = "l", main = "Gilded Aggregated Sunday", ylab = "Frequency")
