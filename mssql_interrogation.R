library('RODBC')
library('dplyr')
library('ggplot2')
library('lubridate')
library(dygraphs)
library(stringr)
library(sqldf)
library(parallel)
library(multidplyr)

#http://www.unixodbc.org/doc/FreeTDS.html for setup guidelines

dbconnection <- odbcConnect("MSSQLServer_W",uid="userid",pwd="password")

fetchData<- sqlQuery(dbconnection, "select TIMEFRAME,USERNAME,USER_DEPARTMENT,LOCATION_CITY,
                                        APPLICATION_NAME,ACTIVITY_NAME,ACTIVITY_RESPONSE_TIME,SERVER_HOSTNAME,SERVER_IP,
                                          SERVER_NAME from aternitymark.dbo.BUSINESS_ACTIVITIES_RAW where TIMEFRAME between '2019-06-24' and '2019-07-07'")

fetchData_newcastle<- sqlQuery(dbconnection, "select TIMEFRAME,USERNAME,USER_DEPARTMENT,LOCATION_CITY,
                                        APPLICATION_NAME,ACTIVITY_NAME,ACTIVITY_RESPONSE_TIME,SERVER_HOSTNAME,SERVER_IP,
                                          SERVER_NAME from aternitymark.dbo.BUSINESS_ACTIVITIES_RAW where TIMEFRAME between '2019-06-24' and '2019-07-15' and LOCATION_CITY like '%newcastle%'")

fetchData_<- sqlQuery(dbconnection, "select * from aternity.dbo.kmoh_subset")

load("fetchData2.RData")
#fetchData<-fetchData %>% filter(APPLICATION_NAME %in% c('EF','CaseFlow','NPS PAYE','ETMP'))
fetchData$TIMEFRAME<-ymd_hms(fetchData$TIMEFRAME)
fetchData$t_month<-month(fetchData$TIMEFRAME, label = TRUE, abbr = TRUE)
fetchData$t_day<-day(fetchData$TIMEFRAME)
fetchData$t_wday<-wday(fetchData$TIMEFRAME, label = TRUE, abbr = TRUE)
fetchData$t_hour<-hour(fetchData$TIMEFRAME)
fetchData_preston<-fetchData %>% filter(str_detect(LOCATION_CITY,fixed('preston', ignore_case=TRUE))) %>% 
  mutate(location_=strsplit(as.character(LOCATION_CITY),"\\\\"))
fetchData_swansea<-fetchData %>% filter(str_detect(LOCATION_CITY,fixed('swansea', ignore_case=TRUE))) %>% 
  mutate(location_=strsplit(as.character(LOCATION_CITY),"\\\\"))

#fetchData %>% group_by(wday(TIMEFRAME,label = TRUE)) %>% summarise(n=n_distinct(USERNAME))
#fetchData %>% filter(TIMEFRAME %within% z) %>%  group_by(wday(TIMEFRAME,label = TRUE), APPLICATION_NAME,USER_DEPARTMENT) %>% 
# summarise(n=n_distinct(USERNAME))
#fetchData %>% filter(date(TIMEFRAME) %within% z) %>%  group_by(APPLICATION_NAME,ACTIVITY_NAME) %>% summarise(n=n())

#drops <- c("x","z")
#DF[ , !(names(DF) %in% drops)]
####################################################################################
outages<-read.csv('outages.csv',header = TRUE)
outages<-outages %>% mutate(location_=strsplit(as.character(Short.Description)," at "))
outages$location__<-lapply(outages$location_,function(x){return(x[2])})
outages<-outages %>% mutate(location___=strsplit(as.character(Short.Description)," to "))
outages$location____<-lapply(outages$location___,function(x){return(x[2])})
outages$location<-coalesce(outages$location__,outages$location____)
outages$Inc.End.Tstamp<-dmy_hm(paste(outages$Inc.End.Date,':',outages$Inc.End.Time,sep=' '))
outages$Inc.Start.Tstamp<-dmy_hm(paste(outages$Inc.Start.Date,':',outages$Inc.Start.Time,sep=' '))
outages$Inc.Duration<-(outages$Inc.End.Tstamp-outages$Inc.Start.Tstamp)/3600
