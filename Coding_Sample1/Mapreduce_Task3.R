### Initialization of Rhipe and Hadoop

Sys.setenv(HADOOP="/data/hadoop")
Sys.setenv(HADOOP_HOME="/data/hadoop")
Sys.setenv(HADOOP_BIN="/data/hadoop/bin") 
Sys.setenv(HADOOP_CMD="/data/hadoop/bin/hadoop") 
Sys.setenv(HADOOP_CONF_DIR="/data/hadoop/etc/hadoop") 
Sys.setenv(HADOOP_LIBS=system("/data/hadoop/bin/hadoop classpath | tr -d '*'",TRUE))


if (!("Rhipe" %in% installed.packages()))
{
  install.packages("/data/hadoop/rhipe/Rhipe_0.75.1.6_hadoop-2.tar.gz", repos=NULL)
}

library(Rhipe)
rhinit()

## Uncomment following lines if you need non-base packages
rhoptions(zips = '/R/R.Pkg.tar.gz')
rhoptions(runner = 'sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh')



### Word Count Example


user_reduce = expression(
  pre = {
    total = 0
  },
  reduce = {
    total = total + sum(unlist(reduce.values))
  },
  post = {
    rhcollect(reduce.key, total)
  }
)

user_map = expression({
  suppressMessages(library(jsonlite))
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      day=as.Date(as.POSIXct(as.numeric(fromJSON(map.values[[r]])$created_utc),origin="1970-01-01",gtz="GMT"))
      if (day=="2015-02-14" | day=="2015-02-27" | day=="2015-02-02") {
        line = tolower(fromJSON(map.values[[r]])$body)
        line = gsub("[-—]"," ",line)
        line = gsub("[^'`’[:alpha:][:space:]]","",line,perl=TRUE)
        line = gsub("(^\\s+|\\s+$)","",line)
        line = strsplit(line, "\\s+")[[1]]
        line = line[line != ""]
        key = list(time=day,comment=line)
        value = 1
        rhcollect(key, value)
      }
      
    }
  )
})

user_map2 = expression({
  suppressMessages(library(jsonlite))
  lapply(
    seq_along(map.keys), 
    function(r) 
    {
      day=as.Date(as.POSIXct(as.numeric(fromJSON(map.values[[r]])$created_utc),origin="1970-01-01",gtz="GMT"))
      if (day=="2015-02-14" | day=="2015-02-27" | day=="2015-02-02") {
        key = list(time=day,comment=fromJSON(map.values[[r]])$body)
        value = 1
        rhcollect(key, value)
      }
      
    }
  )
})


user2 = rhwatch(
  map      = user_map,
  reduce   = user_reduce,
  input    = rhfmt("/data/RC_2015-02.json", type = "text")
)


get_time = function(x,i) as.character(x[[i]]$time)
get_comment=function(x,i) as.character(x[[i]]$comment)
get_val=function(x,i) x[[i]]

counts = data.frame(key = sapply(user2,get_time,i=1),comment = sapply(user2,get_comment,i=1), 
                    value=sapply(user2,get_val,i=2), stringsAsFactors=FALSE)

library(tm)
library(SnowballC)
library(stringr)

n.counts = length(counts)
counts[1:5]

counts$comment=str_replace_all(counts$comment,"[^[:graph:]]", " ") 
counts_0202=counts[which(counts$key=="2015-02-02"),]
counts_0214=counts[which(counts$key=="2015-02-14"),]
counts_0227=counts[which(counts$key=="2015-02-27"),]

library(magrittr)
library(stringr)

word_count_map_no_short = function(x)
{
  x=gsub("[[:digit:]]", "", x)
  x = gsub("[ \t]{2,}", " ", x)
  x = gsub("^\\s+|\\s+$", "", x)
  x %>% tolower() %>%
    str_replace_all("[[:punct:]]","") %>%
    str_split(" ") %>% 
    unlist() %>% 
    str_trim() %>%
    .[. != ""] %>%
    .[nchar(.) > 3] %>%
    base::table() 
}

freq_0214= word_count_map_no_short(counts_0214$comment)
freq_0202= word_count_map_no_short(counts_0202$comment)
freq_0227= word_count_map_no_short(counts_0227$comment)

library(tm)

stopwords=c(stopwords("en"),"just", "dont","thats","didnt","doesnt","thats","like",
            "people","will","think","really","also","even","cant","still","youre",
            "know","much","something","make","going","something","right",
            "well","take","thing","though","first","need","back","sure","better","never"
            ,"things","probably","isnt")

#Valentines Day Word Count
valentine_freq=as.matrix(sort(freq_0214,decreasing = T))
valentine_freq_df=data.frame(valentine_freq)
valentine_freq_df$words=row.names(valentine_freq_df)
##Remove meaningless frequent words
valentine_freq_df=valentine_freq_df[-which(valentine_freq_df$words %in% stopwords),]

#Before Valentines Day Word Count
valentine_freq_02=as.matrix(sort(freq_0202,decreasing = T))
valentine_freq_df_02=data.frame(valentine_freq_02)
valentine_freq_df_02$words=row.names(valentine_freq_df_02)
##Remove meaningless frequent words
valentine_freq_df_02=valentine_freq_df_02[-which(valentine_freq_df_02$words %in% stopwords),]

#After Valentine's Day Word Count
valentine_freq_27=as.matrix(sort(freq_0227,decreasing = T))
valentine_freq_df_27=data.frame(valentine_freq_27)
valentine_freq_df_27$words=row.names(valentine_freq_df_27)
##Remove meaningless frequent words
valentine_freq_df_27=valentine_freq_df_27[-which(valentine_freq_df_27$words %in% stopwords),]

#Visulization of Word Frequency by Word Cloud
wordcloud(words = valentine_freq_df$words,random.color = TRUE, colors=rainbow(10), freq = valentine_freq_df$valentine_freq, min.freq = 5000, random.order = F)
wordcloud(words = valentine_freq_df_02$words,random.color = TRUE, colors=rainbow(10), freq = valentine_freq_df_02$valentine_freq_02, min.freq = 5000, random.order = F)
wordcloud(words = valentine_freq_df_27$words,random.color = TRUE, colors=rainbow(10), freq = valentine_freq_df_27$valentine_freq_27, min.freq = 5000, random.order = F)






