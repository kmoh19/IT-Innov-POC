#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
###########################################################################
##########################                         ########################
##########################                         ########################
##########################   Outages               ########################                                                         
##########################                         ########################
###########################################################################

library(shiny)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(DT)
library(dygraphs)
library(xts)




ui <- fluidPage(
  
  # Application title
  titlePanel("Power Outages POC"),
  
  # Sidebar
  sidebarLayout(
    sidebarPanel(width = 4,
                 #dateInput("Sdate", "Start Date:",value = ymd('2019-06-24')),
                 #dateInput("Edate", "End Date:",value = ymd('2019-06-24')+days(7)),
                 h4("Total Person Hours Lost"),
                 h2(textOutput("Man_Hrs")),
                 br(),
                 h4("Top 5 Affected Departments (%)"),
                 br(),
                 plotOutput("distPlot2"),
                 br(),
                 h4("Distinct Users Per Application/Hour"),
                 plotOutput("distPlot3")
                 
    ),
    
    # Show a plot of the generated distribution
    mainPanel(navbarPage(
      title = selectInput("location", "Location:",
                          c("Preston" = "Preston",
                            "Newcastle" = "Newcastle upon Tyne")),
      tabPanel(h4("Usage"),
               h3("Average Number of Users Per WeekDay/Application"),
               
               plotOutput("distPlot"),
               h3("Two Week Usage Pattern At Location"),
               dygraphOutput("distPlot4")
               
      )
      
    )))
)

# Define server logic required to draw charts
server <- function(input, output) {
  
  load('fetchData_preston.RData')
  load('fetchData_newcastle.RData')
  
  #fetchData<- rbind(fetchData_preston,fetchData_newcastle)
  fetchData_preston$site<- sapply(fetchData_preston$location_,function(x){return(trimws(x[3]))})
  fetchData_newcastle$site<- sapply(fetchData_newcastle$location_,function(x){return(trimws(x[3]))})
  drops <- c("location_")
  fetchData_preston<-fetchData_preston[ , !(names(fetchData_preston) %in% drops)]
  fetchData_newcastle<-fetchData_newcastle[ , !(names(fetchData_newcastle) %in% drops)]
  
  
  
  #top 10 apps
  output$distPlot <- renderPlot({
    top_10<-(if (input$location=='Preston') fetchData_preston else fetchData_newcastle) %>% group_by(APPLICATION_NAME) %>% summarise(n=n_distinct(USERNAME)) %>% arrange(desc(n)) %>% top_n(10) %>% select(APPLICATION_NAME)
    z <-interval(ymd('2019-06-24'),ymd('2019-06-24')+days(7))
    x <- (if (input$location=='Preston') fetchData_preston else fetchData_newcastle) %>% filter(date(TIMEFRAME) %within% z) %>%  group_by(tf=date(TIMEFRAME),APPLICATION_NAME) %>% summarise(n=n_distinct(USERNAME))%>%  group_by(d=wday(tf,label = TRUE),APPLICATION_NAME) %>% summarise(u=round(mean(n),2)) %>% filter(APPLICATION_NAME %in% top_10$APPLICATION_NAME)
    ggplot(x,aes(x=d,y=u,group=1))+geom_line(color="Orange")+facet_wrap(.~APPLICATION_NAME)+geom_text(aes(label=u), position=position_dodge(width=0.9), vjust=-0.25)
    
  })
  
  # output$distPlot1<-renderPlot({
  #     z <-interval(input$Sdate,input$Edate)
  #     x1 <- fetchData %>% filter(date(TIMEFRAME) %within% z) %>%  
  #         group_by(tf=date(TIMEFRAME),APPLICATION_NAME) %>% summarise(n=n_distinct(USERNAME))%>% 
  #         group_by(w=wday(tf,label = TRUE),APPLICATION_NAME) %>% summarise(u=round(mean(n),2))
  #     
  #     x2 <- fetchData %>% filter(date(TIMEFRAME) %within% z) %>% 
  #         group_by(d=date(TIMEFRAME),APPLICATION_NAME) %>% summarise(n=n_distinct(USERNAME))%>% mutate(w=wday(d,label = TRUE))
  #     x<-merge(x1,x2, by=c("APPLICATION_NAME","w"))%>% filter(APPLICATION_NAME %in% top_10$APPLICATION_NAME)
  #     ggplot(x,aes(x=d,y=n))+geom_bar(stat = "identity")+facet_wrap(.~APPLICATION_NAME)+geom_text(aes(label=n), position=position_dodge(width=0.9), vjust=-0.25)+
  #         geom_errorbar(aes(ymin = u, ymax = u), color="Orange",lty=2)
  #     
  # })
  
  #dygrap
  output$distPlot4<-renderDygraph({
    
    x<-(if (input$location=='Preston') fetchData_preston else fetchData_newcastle) %>%drop_na() %>% mutate(t_date=round_date(TIMEFRAME, "5 mins")) %>% group_by(t_date) %>% summarise(n=n_distinct(USERNAME))
    x<-xts(x[,-1],order.by = x$t_date)
    dygraph(x) %>% dyRangeSelector() %>%
      dyOptions(fillGraph = TRUE, fillAlpha = 0.4)
    
    
  })
  
  #app breakdown
  output$distPlot3<-renderPlot({
    top_10<-(if (input$location=='Preston') fetchData_preston else fetchData_newcastle)%>%group_by(APPLICATION_NAME) %>% summarise(n=n_distinct(USERNAME)) %>% arrange(desc(n)) %>% top_n(10) %>% select(APPLICATION_NAME)
    x<- (if (input$location=='Preston') fetchData_preston else fetchData_newcastle)%>%drop_na() %>% filter(date(TIMEFRAME)=='2019-06-28') %>% group_by(t_hour,APPLICATION_NAME) %>% #########################'2019-07-04'
      summarise(n=n_distinct(USERNAME)) %>% filter(APPLICATION_NAME %in% top_10$APPLICATION_NAME)%>% arrange(t_hour)
    
    ggplot(x, aes(fill=APPLICATION_NAME, y=n, x=t_hour)) + 
      geom_bar( stat="identity")+scale_fill_brewer(palette = "Set3")
  })
  
  #donut 
  output$distPlot2<-renderPlot({
    
    z<- interval(ymd_hms('2019-06-28 07:25:00'),ymd_hms('2019-06-28 08:25:00'))  ################################################ 
    mycols <- c("#0073C2FF", "#EFC000FF", "#868686FF","#E5E5E5FF","#A6761D","#CD534CFF","#BC534CFF")
    
    top_5<-(if (input$location=='Preston') fetchData_preston else fetchData_newcastle)%>%drop_na() %>%  filter(TIMEFRAME %within% z) %>% 
      select(USER_DEPARTMENT,USERNAME) %>% group_by(USER_DEPARTMENT) %>% summarise(n=n_distinct(USERNAME)) %>% arrange(desc(n))%>% top_n(5)
    
    Data <- (if (input$location=='Preston') fetchData_preston else fetchData_newcastle) %>%drop_na() %>% filter(TIMEFRAME %within% z) %>% 
      merge(top_5,by='USER_DEPARTMENT', all=TRUE) %>%mutate(USER_DEPARTMENT=ifelse(is.na(n),'Others',as.character(USER_DEPARTMENT))) %>% 
      select(USER_DEPARTMENT,USERNAME) %>% group_by(USER_DEPARTMENT) %>% summarise(n=n_distinct(USERNAME)) %>% mutate(prop=round(100*n/sum(n),2)) %>% 
      arrange(desc(USER_DEPARTMENT)) %>% mutate(lab.ypos = cumsum(prop) - 0.5*prop)
    
    
    ggplot(Data, aes(x = 2, y = prop, fill = USER_DEPARTMENT)) +
      geom_bar(stat = "identity", color = "white") +
      coord_polar(theta = "y", start = 0)+
      geom_text(aes(y = lab.ypos, label = prop), color = "white")+
      scale_fill_manual(values = mycols) +
      theme_void()+
      xlim(0.5, 2.5)
  })
  
  #man hours
  output$Man_Hrs<-renderText({
    z<- interval(ymd_hms('2019-06-28 07:25:00'),ymd_hms('2019-06-28 08:25:00'))
    x<-(if (input$location=='Preston') fetchData_preston else fetchData_newcastle) %>%drop_na() %>% filter(TIMEFRAME %within% z)
    paste(count(distinct(as.data.frame(x$USERNAME)))*2.00,' Hours')##########################change to variable
    
  })
  
}




# Run the application 
shinyApp(ui = ui, server = server)