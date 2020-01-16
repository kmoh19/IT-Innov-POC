library(odbc)
library(tidyverse)
library(dplyr)
library(ggplot2)
library(lubridate)
library(dygraphs)
library(stringr)

############################## Creating Athena Pipeline ###########################################################

Sys.setenv("AWS_ACCESS_KEY_ID" = "your_id",
           "AWS_SECRET_ACCESS_KEY" = "your_key",
           "AWS_DEFAULT_REGION" = "eu-west-2")

DBI::dbConnect(
  odbc::odbc(), 
  driver = "/opt/simba/athenaodbc/lib/64/libathenaodbc_sb64.so", 
  AwsRegion = Sys.getenv("AWS_DEFAULT_REGION"),
  AuthenticationType = "IAM Credentials",
  S3OutputLocation = "s3://S3_Output_Location/",
  UID = Sys.getenv("AWS_ACCESS_KEY_ID"),
  PWD = Sys.getenv("AWS_SECRET_ACCESS_KEY")
) -> con



DBI::dbSendQuery(con,"CREATE DATABASE IF NOT EXISTS analysesdb")

DBI::dbSendQuery(con,"CREATE EXTERNAL TABLE IF NOT EXISTS analysesdb.BUSINESS_ACTIVITIES_RAW
                 (ACTIVITY_DETECTION_STATUS   VARCHAR(255),
              PAGE_LOAD_REDIRECT_TIME   VARCHAR(255),
              DEVICE_CPU_CORES   VARCHAR(255),
              TIMEFRAME   VARCHAR(255),
              APPLICATION_NAME   VARCHAR(255),
              NETWORK_OUTGOING_TRAFFIC_TOTAL   VARCHAR(255),
              VIRTUALIZATION   VARCHAR(255),
              WIFI_BSSID   VARCHAR(255),
              CORP_MARKET   VARCHAR(255),
              DEVICE_CPU_GENERATION   VARCHAR(255),
              APPLICATION_VERSION   VARCHAR(255),
              CORP_LINE_OF_BUSINESS   VARCHAR(255),
              ACTIVITY_PAGE_TITLE   VARCHAR(255),
              PRC_CPU_UTIL   VARCHAR(255),
              PRC_VIRTUAL_MEMORY_CONSUMED   VARCHAR(255),
              MEASUREMENT_START_TIMESTAMP   VARCHAR(255),
              HRC_DISK_IO_WRITE_RATE   VARCHAR(255),
              DEVICE_NETWORK_TYPE   VARCHAR(255),
              PRC_PHYSICAL_MEMORY_CONSUMED   VARCHAR(255),
              USER_DEPARTMENT   VARCHAR(255),
              CLIENT_DEVICE_TYPE   VARCHAR(255),
              DEVICE_IP_ADDRESS   VARCHAR(255),
              OS_ARCHITECTURE   VARCHAR(255),
              OS_FAMILY   VARCHAR(255),
              ACTIVITY_RESPONSE_TIME   VARCHAR(255),
              ACTIVITY_ADDITIONAL_INFO1   VARCHAR(255),
              ACTIVITY_ADDITIONAL_INFO2   VARCHAR(255),
              PAGE_LOAD_REQUEST_TIME   VARCHAR(255),
              WIFI_SSID   VARCHAR(255),
              PAGE_LOAD_PROCESSING_TIME   VARCHAR(255),
              USER_DOMAIN   VARCHAR(255),
              OS_NAME   VARCHAR(255),
              DEVICE_MEMORY   VARCHAR(255),
              DEVICE_MANUFACTURER   VARCHAR(255),
              USER_TITLE   VARCHAR(255),
              LOCATION_ON_VPN   VARCHAR(255),
              PAGE_LOAD_RESPONSE_TIME   VARCHAR(255),
              CALENDAR_DATE   VARCHAR(255),
              LOCATION_REGION   VARCHAR(255),
              NETWORK_INCOMING_TRAFFIC_TOTAL   VARCHAR(255),
              ACTIVITY_CLIENT_TIME   VARCHAR(255),
              ACTIVITY_NAME   VARCHAR(255),
              SERVER_HOSTNAME   VARCHAR(255),
              MOBILE_CARRIER   VARCHAR(255),
              PAGE_LOAD_TIME   VARCHAR(255),
              USER_ROLE   VARCHAR(255),
              OS_VERSION   VARCHAR(255),
              NETWORK_RTT_AVG   VARCHAR(255),
              BROWSER   VARCHAR(255),
              APPLICATION_TYPE   VARCHAR(255),
              ACCOUNT_NAME   VARCHAR(255),
              DEVICE_ID_MOBILE_OR_MAC   VARCHAR(255),
              MS_OFFICE_VERSION   VARCHAR(255),
              CUSTOM_ATTRIBUTE_3   VARCHAR(255),
              CUSTOM_ATTRIBUTE_2   VARCHAR(255),
              CUSTOM_ATTRIBUTE_1   VARCHAR(255),
              CUSTOM_ATTRIBUTE_6   VARCHAR(255),
              CUSTOM_ATTRIBUTE_5   VARCHAR(255),
              DEVICE_SUBNET   VARCHAR(255),
              ACTIVITY_DIVERSE_VALUE_2   VARCHAR(255),
              ACTIVITY_NETWORK_TIME   VARCHAR(255),
              USER_EMAIL_ADDRESS   VARCHAR(255),
              DEVICE_TYPE   VARCHAR(255),
              HRC_VIRTUAL_MEMORY_UTIL   VARCHAR(255),
              OS_DISK_TYPE   VARCHAR(255),
              ACTIVITY_DIVERSE_VALUE_1   VARCHAR(255),
              BUSINESS_LOCATION   VARCHAR(255),
              ACTIVITY_DIVERSE_VALUE_3   VARCHAR(255),
              CALENDAR_MONTH   VARCHAR(255),
              PAGE_LOAD_HTTP_STATUS_CODE   VARCHAR(255),
              USER_OFFICE   VARCHAR(255),
              HRC_DISK_IO_READ_RATE   VARCHAR(255),
              USER_FULL_NAME   VARCHAR(255),
              CORP_STORE_ID   VARCHAR(255),
              USERNAME   VARCHAR(255),
              HRC_PHYSICAL_MEMORY_UTIL   VARCHAR(255),
              DEVICE_MODEL   VARCHAR(255),
              DATA_CENTER_BUSINESS_LOCATION   VARCHAR(255),
              DEVICE_CPU_TYPE   VARCHAR(255),
              CALENDAR_WEEK   VARCHAR(255),
              WIFI_CHANNEL   VARCHAR(255),
              PAGE_LOAD_TCP_CONNECT_TIME   VARCHAR(255),
              SLA_STATUS   VARCHAR(255),
              DEVICE_CPU_FREQUENCY   VARCHAR(255),
              HRC_CPU_UTIL   VARCHAR(255),
              LOCATION_STATE   VARCHAR(255),
              ACTIVITY_REMOTE_DISPLAY_LATENCY_AVG   VARCHAR(255),
              LOCATION_COUNTRY   VARCHAR(255),
              HRC_NETWORK_IO_READ_RATE   VARCHAR(255),
              HRC_DISK_QUEUE_LENGTH   VARCHAR(255),
              HRC_NETWORK_IO_WRITE_RATE   VARCHAR(255),
              CUSTOM_PILOT_GROUP   VARCHAR(255),
              SERVER_NAME   VARCHAR(255),
              PAGE_LOAD_DNS_TIME   VARCHAR(255),
              DEVICE_NAME   VARCHAR(255),
              DEVICE_IMAGE_BUILD_NUMBER   VARCHAR(255),
              DEVICE_CPU_MODEL   VARCHAR(255),
              DEVICE_POWER_PLAN   VARCHAR(255),
              LOCATION_CITY   VARCHAR(255),
              CLIENT_DEVICE_NAME   VARCHAR(255),
              MS_OFFICE_LICENSE_TYPE   VARCHAR(255),
              DEVICE_DAYS_FROM_LAST_BOOT   VARCHAR(255),
              ACCOUNT_ID   VARCHAR(255),
              CORP_CHANNEL   VARCHAR(255),
              CORP_STORE_TYPE   VARCHAR(255),
              LOCATION_ON_SITE   VARCHAR(255),
              SERVER_IP   VARCHAR(255),
              CUSTOM_ATTRIBUTE_4   VARCHAR(255),
              ACTIVITY_BACKEND_TIME   VARCHAR(255)
)
                 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                 WITH SERDEPROPERTIES ('separatorChar' = ',' ,'quoteChar' = '\"')
                 LOCATION 's3://CSV_Location/' 
                 TBLPROPERTIES('skip.header.line.count'='1');")


DBI::dbSendQuery(con,"CREATE EXTERNAL TABLE IF NOT EXISTS analysesdb.APPLICATIONS_RAW
                  (CORP_LINE_OF_BUSINESS   VARCHAR(255),
                  DEVICE_CPU_CORES   VARCHAR(255),
                  TIMEFRAME   VARCHAR(255),
                  APPLICATION_CRASHES_TOTAL   VARCHAR(255),
                  VIRTUALIZATION   VARCHAR(255),
                  WIFI_BSSID   VARCHAR(255),
                  CORP_MARKET   VARCHAR(255),
                  DEVICE_CPU_GENERATION   VARCHAR(255),
                  APPLICATION_VERSION   VARCHAR(255),
                  PAGE_LOAD_CLIENT_TIME_AVG   VARCHAR(255),
                  ACTIVITY_RESPONSE_TIME_AVG   VARCHAR(255),
                  APPLICATIONS_ERRORS_TOTAL   VARCHAR(255),
                  DEVICE_CPU_MODEL   VARCHAR(255),
                  ACTIVITY_VOLUME   VARCHAR(255),
                  ACTIVITY_CLIENT_TIME_AVG   VARCHAR(255),
                  DEVICE_NETWORK_TYPE   VARCHAR(255),
                  USER_DEPARTMENT   VARCHAR(255),
                  APPLICATION_IDENTIFIER   VARCHAR(255),
                  CLIENT_DEVICE_TYPE   VARCHAR(255),
                  DEVICE_IP_ADDRESS   VARCHAR(255),
                  OS_ARCHITECTURE   VARCHAR(255),
                  OS_FAMILY   VARCHAR(255),
                  USER_OFFICE   VARCHAR(255),
                  APPLICATION_NAME   VARCHAR(255),
                  ACTIVITY_NETWORK_TIME_AVG   VARCHAR(255),
                  PAGE_LOAD_NETWORK_TIME_AVG   VARCHAR(255),
                  PAGE_LOAD_VOLUME   VARCHAR(255),
                  USER_DOMAIN   VARCHAR(255),
                  OS_NAME   VARCHAR(255),
                  DEVICE_MEMORY   VARCHAR(255),
                  DEVICE_MANUFACTURER   VARCHAR(255),
                  USER_TITLE   VARCHAR(255),
                  LOCATION_ON_VPN   VARCHAR(255),
                  CALENDAR_DATE   VARCHAR(255),
                  APPLICATION_WAIT_TIME_TOTAL   VARCHAR(255),
                  LOCATION_REGION   VARCHAR(255),
                  APPLICATION_HANG_TIME_TOTAL   VARCHAR(255),
                  WIFI_SSID   VARCHAR(255),
                  ACTIVITY_SCORE   VARCHAR(255),
                  MOBILE_CARRIER   VARCHAR(255),
                  USER_ROLE   VARCHAR(255),
                  OS_VERSION   VARCHAR(255),
                  BROWSER   VARCHAR(255),
                  APPLICATION_TYPE   VARCHAR(255),
                  DEVICE_ID_MOBILE_OR_MAC   VARCHAR(255),
                  MS_OFFICE_VERSION   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_3   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_2   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_1   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_6   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_5   VARCHAR(255),
                  DEVICE_SUBNET   VARCHAR(255),
                  APPLICATION_UXI_AVG   VARCHAR(255),
                  USER_EMAIL_ADDRESS   VARCHAR(255),
                  APPLICATION_UXI_WEIGHT_AVG   VARCHAR(255),
                  DEVICE_TYPE   VARCHAR(255),
                  OS_DISK_TYPE   VARCHAR(255),
                  BUSINESS_LOCATION   VARCHAR(255),
                  CALENDAR_MONTH   VARCHAR(255),
                  APPLICATION_USAGE_TIME_TOTAL   VARCHAR(255),
                  USER_FULL_NAME   VARCHAR(255),
                  CORP_STORE_ID   VARCHAR(255),
                  USERNAME   VARCHAR(255),
                  DEVICE_MODEL   VARCHAR(255),
                  ACTIVITY_BACKEND_TIME_AVG   VARCHAR(255),
                  DATA_CENTER_BUSINESS_LOCATION   VARCHAR(255),
                  DEVICE_CPU_TYPE   VARCHAR(255),
                  CALENDAR_WEEK   VARCHAR(255),
                  WIFI_CHANNEL   VARCHAR(255),
                  PAGE_LOAD_BACKEND_TIME_AVG   VARCHAR(255),
                  DEVICE_CPU_FREQUENCY   VARCHAR(255),
                  ACCOUNT_NAME   VARCHAR(255),
                  LOCATION_STATE   VARCHAR(255),
                  ACTIVITY_REMOTE_DISPLAY_LATENCY_AVG   VARCHAR(255),
                  DEVICE_NAME   VARCHAR(255),
                  LOCATION_COUNTRY   VARCHAR(255),
                  CUSTOM_PILOT_GROUP   VARCHAR(255),
                  PAGE_LOAD_TIME_AVG   VARCHAR(255),
                  DEVICE_IMAGE_BUILD_NUMBER   VARCHAR(255),
                  APPLICATION_ACTIVE_TIME_TOTAL   VARCHAR(255),
                  DEVICE_POWER_PLAN   VARCHAR(255),
                  LOCATION_CITY   VARCHAR(255),
                  CLIENT_DEVICE_NAME   VARCHAR(255),
                  MS_OFFICE_LICENSE_TYPE   VARCHAR(255),
                  DEVICE_DAYS_FROM_LAST_BOOT   VARCHAR(255),
                  ACCOUNT_ID   VARCHAR(255),
                  CORP_CHANNEL   VARCHAR(255),
                  CORP_STORE_TYPE   VARCHAR(255),
                  LOCATION_ON_SITE   VARCHAR(255),
                  CUSTOM_ATTRIBUTE_4   VARCHAR(255)
)
                  
                 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                 WITH SERDEPROPERTIES ('separatorChar' = ',' ,'quoteChar' = '\"')
                 LOCATION 's3://CSV_Location/' 
                 TBLPROPERTIES('skip.header.line.count'='1');")


a<-DBI::dbGetQuery(con,"SELECT * FROM analysesdb.BUSINESS_ACTIVITIES_RAW LIMIT 100")
View(a)

b<-DBI::dbGetQuery(con,"SELECT * FROM analysesdb.APPLICATIONS_RAW LIMIT 100")
View(b)


query_0<-"CREATE TABLE analysesdb.APP_RAW_2WK
WITH (
  format='PARQUET',
  external_location='s3://aternity-ingestion-storage/ANALYSES/TABLES_APP_RAW_2WK'
) AS      
        SELECT USERNAME,
             USER_DEPARTMENT,
             BUSINESS_LOCATION,
             APPLICATION_NAME,
             DATA_CENTER_BUSINESS_LOCATION,
             APPLICATION_TYPE,
             APPLICATION_ACTIVE_TIME_TOTAL,
             APPLICATION_USAGE_TIME_TOTAL,
             TIMEFRAME,
             DEVICE_NETWORK_TYPE,
             CALENDAR_DATE
        FROM analysesdb.APPLICATIONS_RAW
        WHERE date_parse(CALENDAR_DATE, '%Y/%m/%d') >= timestamp '2019-07-29'
                AND date_parse(CALENDAR_DATE, '%Y/%m/%d') <= timestamp '2019-08-11'"


query_1<-"CREATE TABLE analysesdb.APP_RAW_15_27
WITH (
  format='PARQUET',
  external_location='s3://S3_Output_Location/ANALYSES/TABLES_APP_RAW_15_27'
) AS      
        SELECT USERNAME,
             USER_DEPARTMENT,
             BUSINESS_LOCATION,
             APPLICATION_NAME,
             DATA_CENTER_BUSINESS_LOCATION,
             APPLICATION_ACTIVE_TIME_TOTAL,
             APPLICATION_USAGE_TIME_TOTAL,
             APPLICATION_TYPE,
             TIMEFRAME,
             DEVICE_NETWORK_TYPE,
             CALENDAR_DATE
        FROM analysesdb.APPLICATIONS_RAW
        WHERE date_parse(CALENDAR_DATE, '%Y/%m/%d') >= timestamp '2019-07-15'
                AND date_parse(CALENDAR_DATE, '%Y/%m/%d') <= timestamp '2019-07-27'"


query_2<-"CREATE TABLE analysesdb.cesa_23072017
WITH (
  format='PARQUET',
  external_location='s3://S3_Output_Location/ANALYSES/TABLES'
) AS
SELECT TIMEFRAME,
        USERNAME,
        USER_DEPARTMENT,
        LOCATION_CITY,
        BUSINESS_LOCATION,
        APPLICATION_NAME,
        ACTIVITY_NAME,
        ACTIVITY_RESPONSE_TIME,
        SERVER_HOSTNAME,
        SERVER_IP,
        SERVER_NAME
FROM analysesdb.cesa
WHERE CALENDAR_DATE ='2019/07/23'"

query_3<-"CREATE TABLE analysesdb.cesa_24072019
WITH (
  format='PARQUET',
  external_location='s3://S3_Output_Location/ANALYSES/TABLES_cesa_24072019'
) AS
SELECT TIMEFRAME,
        USERNAME,
        USER_DEPARTMENT,
        LOCATION_CITY,
        BUSINESS_LOCATION,
        APPLICATION_NAME,
        ACTIVITY_NAME,
        ACTIVITY_RESPONSE_TIME,
        SERVER_HOSTNAME,
        SERVER_IP,
        SERVER_NAME
        
FROM analysesdb.cesa
WHERE CALENDAR_DATE ='2019/07/24'"


dbSendQuery(con,query_0)

dbSendQuery(con,query_1)

fetchData<-dbGetQuery(con,'select * from analysesdb.APP_RAW_2WK')
save(fetchData,file = "~/Data/APP_RAW_2WK.RData")

fetchData<- NULL

fetchData<-dbGetQuery(con,'select * from analysesdb.APP_RAW_15_27')
save(fetchData,file ="~/Data/APP_RAW_15_27.RData")


######################################################## Data Munging #########################################################

cesa<-c()

fetchData<-fetchData %>% filter(application_name %in% cesa)
fetchData$t<-str_split(fetchData$timeframe,"[+]")
fetchData$t<-lapply(fetchData$t,function(x){return(x[1])})
fetchData$timeframe<-ymd_hms(fetchData$t)
fetchData$t_month<-month(fetchData$timeframe, label = TRUE, abbr = TRUE)
fetchData$t_day<-day(fetchData$timeframe)
fetchData$t_wday<-wday(fetchData$timeframe, label = TRUE, abbr = TRUE)
fetchData$t_hour<-hour(fetchData$timeframe)
fetchData$t<-str_split(fetchData$business_location,"[,]")
fetchData$site<-sapply(fetchData$t,function(x){return(x[1])})
fetchData$city_town<-sapply(fetchData$t,function(x){return(x[2])})
fetchData %>% group_by(site,user_department) %>% summarise(n=n_distinct(username))


############################################################################################
load(file="~/Data/APP_RAW_15_27.RData")
data_23rd<-fetchData %>% filter(calendar_date %in% c('2019/07/22','2019/07/23','2019/07/24','2019/07/25'))
data_23rd$t<-str_split(data_23rd$timeframe,"[+]")
data_23rd$t<-sapply(data_23rd$t,function(x){return(x[1])})
data_23rd$timeframe<-ymd_hms(data_23rd$t)
data_23rd$t_month<-month(data_23rd$timeframe, label = TRUE, abbr = TRUE)
data_23rd$t_day<-day(data_23rd$timeframe)
data_23rd$t_wday<-wday(data_23rd$timeframe, label = TRUE, abbr = TRUE)
data_23rd$t_hour<-hour(data_23rd$timeframe)
data_23rd$t<-str_split(data_23rd$business_location,"[,]")
data_23rd$site<-sapply(data_23rd$t,function(x){return(x[1])})
data_23rd$city_town<-sapply(data_23rd$t,function(x){return(x[2])})
data_23rd$napp_name<-sapply(data_23rd$application_name,function(x){return(if(x %in% cesa) "CESA" else x)})
rm(list=c("fetchData","fetchData_CESA"))
data_23rd %>% group_by(site,user_department) %>% summarise(n=n_distinct(username))