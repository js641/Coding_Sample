### Task1
# Each comment belongs to a particular subreddit, we would like to 
# know for the *given time period* what were the 25 most popular subreddits? 
# We would also like to have this broken down by month (Jan to May) - 
# create a Billboard-like table for each month that shows the top 25 subreddits 
# for that month that also includes the change in rank since the previous month. 
# Comment on any subreddits that show a strong positive or negative trend.


###########################################################################
########################### Prepare the SPARK #############################
###########################################################################

### Run this if you haven't done so
Sys.setenv(HADOOP_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(YARN_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(SPARK_HOME="/data/hadoop/spark")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R/lib"), .libPaths()))
library(SparkR)

sc = sparkR.init()
#sc = sparkR.init(master="yarn-client")
sqlContext = sparkRSQL.init(sc)

###########################################################################
############### Read and Prepare the 5 Month Data #########################
###########################################################################

# full 5 month data set
j_all_five = jsonFile(sqlContext, "hdfs://localhost:9000/data/RC_2015-0*.json")
registerTempTable(j_all_five, "reddit_all_five")
res_all_five = SparkR::sql(sqlContext, "SELECT subreddit, created_utc FROM reddit_all_five")

# construct a new column time having format "yyyy-MM-dd HH:mm:ss"
res_all_five$time=from_unixtime(res_all_five$created_utc, format = "yyyy-MM-dd HH:mm:ss")

# extract month
res_all_five$month=month(res_all_five$time)

# group the subreddit by subreddit and month
res_all_five = count(group_by(res_all_five,"subreddit","month"))
res_all_five=arrange(res_all_five, desc(res_all_five$count))

# collect the data from SparkR
res_all_five_collected = collect(res_all_five)

# save the data into 5 different dataframe by month: Jan - May
rank_jan = res_all_five_collected[res_all_five_collected$month == 1,]
rank_feb = res_all_five_collected[res_all_five_collected$month == 2,]
rank_mar = res_all_five_collected[res_all_five_collected$month == 3,]
rank_apr = res_all_five_collected[res_all_five_collected$month == 4,]
rank_may = res_all_five_collected[res_all_five_collected$month == 5,]

# add the rank index number as column to each of the 5 month
rank_jan_ind = cbind("rank" = seq(1,dim(rank_jan)[1], by = 1), rank_jan)
rank_feb_ind = cbind("rank" = seq(1,dim(rank_feb)[1], by = 1), rank_feb)
rank_mar_ind = cbind("rank" = seq(1,dim(rank_mar)[1], by = 1), rank_mar)
rank_apr_ind = cbind("rank" = seq(1,dim(rank_apr)[1], by = 1), rank_apr)
rank_may_ind = cbind("rank" = seq(1,dim(rank_may)[1], by = 1), rank_may)


###########################################################################
########### Change in count compared to previous month ####################
###########################################################################

# install.packages("plyr")
library(plyr)

# Functions for comparison, df1 is the previous month, df2 is the current month,
# it will return how the changes comparing df2 to df1 for the top 25 item on df2. 

# The function will have two new columns adding to the current month:
# Column one, called comment. It shows the status of the corresponding 
# subreddit. There are 4 status:
#     TIE: rank remains the same 
#     DOWN: rank decreases
#     UP: rank increases
#     NEW: new subredit item appearing in the current month
# Column two, Called change,  shows rank changes for the corresponding redit:
#     0 for TIE, 
#     negative number for DOWN, 
#     positive number for UP 
#     current rank for the NEW reddit  

rank = function (df1, df2) {
  df1_new = df1
  df2_new = df2
  
  # convert the subreddit to lower case
  df1_new$subreddit = tolower(df1_new$subreddit)
  df2_new$subreddit = tolower(df2_new$subreddit)
  
  # left joint df1 on df2 by subreddit
  df_merge = merge(x = df2_new, y = df1_new, by = "subreddit", all.x = TRUE)
  df_merge = arrange(df_merge, rank.x) 
  
  # add a new column, Comment, to df2, this new column mark the change from the previous moth
  # There are four types of changes: 
  #     TIE: rank remains the same
  #     DOWN: rank decreases
  #     UP: rank increases
  #     NEW: new item in the top 25
  df2$Comment = ""
  df2$Comment[which(df_merge$rank.x == df_merge$rank.y)] = "TIE"
  df2$Comment[which(df_merge$rank.x > df_merge$rank.y)] = "DOWN"
  df2$Comment[which(df_merge$rank.x < df_merge$rank.y)] = "UP"
  df2$Comment[is.na(df_merge$rank.y)] = "NEW"
  
  df2$Change = 0
  df2$Change[which(df_merge$rank.x == df_merge$rank.y)] = 0
  df2$Change[which(df_merge$rank.x > df_merge$rank.y)] = abs(df_merge$rank.x[which(df_merge$rank.x > df_merge$rank.y)] - df_merge$rank.y[which(df_merge$rank.x > df_merge$rank.y)])*(-1)
  df2$Change[which(df_merge$rank.x < df_merge$rank.y)] = abs(df_merge$rank.x[which(df_merge$rank.x < df_merge$rank.y)] - df_merge$rank.y[which(df_merge$rank.x < df_merge$rank.y)])
  df2$Change[is.na(df_merge$rank.y)] = df_merge$rank.x[is.na(df_merge$rank.y)]
  
  return (df2[1:25,])
}

# compare the second to the first functions, comment
# compare top 25 on Feburay to January 
rank(rank_jan_ind, rank_feb_ind)

# compare top 25 on March to Feburary 
rank(rank_feb_ind, rank_mar_ind)

# compare top 25 on April to March 
rank(rank_mar_ind, rank_apr_ind)

# compare top 25 on May to April 
rank(rank_apr_ind, rank_may_ind)
