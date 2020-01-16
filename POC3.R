#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
#Bash script for global package install
#sudo su - -c "R -q -e \"install.packages('mypackage', repos='http://cran.rstudio.com/')\""

###########################################################################
##########################                         ########################
##########################                         ########################
##########################   Cesa Outage           ########################                                                         
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
library(imputeTS) # for na_replace of ts objects
options(shiny.sanitize.errors = FALSE)


ui <- fluidPage(
  
  # Application title
  titlePanel("Cesa Outages"),
  
  # Sidebar
  sidebarLayout(
    sidebarPanel(width = 3,
                 
                 htmlOutput("selectDept"),
                 br(),
                 #htmlOutput("selectCity"),
                 #br(),
                 #htmlOutput("selectOffice")
                 #h4("Top 5 Affected Departments (%)"),
                 #br(),
                 #plotOutput("distPlot2"),
                 #br()
                 #h4("Distinct Users Per Application/Hour"),
                 #plotOutput("distPlot3")
                 h4("Top 5 Affected Departments (%)"),
                 br(),
                 plotOutput("distPlot2")
                 
                 
                 
    ),
    
    # Show a plot of the generated distribution
    mainPanel(navbarPage(
      
      title = 'Analyses',
      tabPanel(h4("Usage"),
               
               h3("CESA Users Activity Profile Top 15 Applications:July 22nd - 25th"),
               dygraphOutput("distPlot4"),
               
               br(),
               
               h3("Number of CESA Users"),
               dygraphOutput("distPlot5"),
               br()
               
               
               
      )
      
    )))
)

# Define server logic required to draw charts
server <- function(input, output) {
  
  
  cesa_users<-data_23rd %>% filter(napp_name=="CESA") %>% select(username) %>% distinct(username)
  cesa_users_data<-merge(cesa_users,data_23rd)
  
  save(cesa_users,file="cesa_users.RData")
  save(cesa_users_data,file="cesa_users_data.RData")
  
  cesa_users_data_<-reactive({if (input$dept!="__all__") cesa_users_data %>% filter(user_department==input$dept)
    else cesa_users_data
    
    
  })
  
  
  #DynamicUI
  
  output$selectDept <- renderUI({
    dept<-cesa_users_data %>% select(user_department) %>% distinct(user_department) %>% rbind(c("__all__")) %>% arrange(user_department)
    selectInput("dept", "User Department", dept, selected = "Benefits & Credits")
    
  })
  
  # output$selectCity <- renderUI({
  #     city<-data_23rd %>% select(city_town) %>% distinct(city_town) %>% drop_na() %>% rbind(c("__all__")) %>% arrange(desc(city_town))
  #     selectInput("city_town", "City", city)
  # })
  # 
  # 
  # output$selectOffice <- renderUI({
  #     office<-data_23rd %>% select(site) %>% distinct(site)%>% rbind(c("__all__")) %>% arrange(site)
  #     selectInput("site", "Office", office)
  # })
  
  
  #dygraph
  output$distPlot4<-renderDygraph({
    
    top_15<-cesa_users_data_() %>% group_by(application_name) %>% summarise(n=sum(as.numeric(application_usage_time_total))) %>% arrange(desc(n)) %>% top_n(15) %>% select(application_name)
    
    # non CESA app data prep
    app_xts<-c()
    for (i in 1:15) {
      app_dat<-cesa_users_data_() %>% filter(application_name==top_15[[1]][i])
      app_dat<-app_dat %>%drop_na() %>% mutate(t_date=round_date(timeframe, "5 mins")) %>% group_by(t_date) %>% summarise(n=sum(as.numeric(application_usage_time_total))/60000)
      app_dat<-xts(app_dat[,-1],order.by = app_dat$t_date)
      colnames(app_dat)<-top_15[[1]][i]
      app_xts<-cbind(app_xts,app_dat)
    }
    
    #   CESA app data prep
    app_dat<-cesa_users_data_() %>% filter(napp_name=="CESA")
    app_dat<-app_dat %>%drop_na() %>% mutate(t_date=round_date(timeframe, "5 mins")) %>% group_by(t_date) %>% summarise(n=sum(as.numeric(application_usage_time_total))/60000)
    app_dat<-xts(app_dat[,-1],order.by = app_dat$t_date)
    colnames(app_dat)<-"CESA"
    app_xts<-cbind(app_dat,app_xts)
    
    app_xts<-na_replace(app_xts, fill = 0, maxgap = Inf)
    cat(app_xts)
    dygraph(app_xts)%>% dyOptions(connectSeparatedPoints = FALSE, stackedGraph = TRUE,fillAlpha = 0.5,colors = RColorBrewer::brewer.pal(12, "Paired")) %>%  dyAxis("y", label = "Total Time (Minutes)") %>% 
      dyRangeSelector(height = 20) %>% dyEvent("2019-07-23 11:05:00", "Outage Reported", labelLoc = "top") #adjusting for time diff 12->11
    
    
  })
  
  
  
  
  output$distPlot5<-renderDygraph({  
    app_dat<-cesa_users_data_() %>% filter(napp_name=="CESA")
    app_dat<-app_dat %>%drop_na() %>% mutate(t_date=round_date(timeframe, "30 mins")) %>% group_by(t_date,application_name) %>%  summarise(n=n_distinct(username))
    app_dat<-spread(app_dat,application_name,n)
    app_dat<-xts(app_dat[,-1],order.by = app_dat$t_date)
    app_dat<-na_replace(app_dat, fill = 0, maxgap = Inf)
    
    
    dygraph(app_dat) %>% dyRangeSelector(height = 20) %>% dyEvent("2019-07-23 11:05:00", "Outage Reported", labelLoc = "top") %>%  dyAxis("y", label = "Count") %>% 
      dyStackedBarGroup(c(names(app_dat)))#%>% dyOptions(colors = RColorBrewer::brewer.pal(9, "Set1"))#adjusting for time diff 12->11
    
  })
  
  #donut 
  output$distPlot2<-renderPlot({
    
    mycols <- c("#0073C2FF", "#EFC000FF", "#868686FF","#E5E5E5FF","#A6761D","#CD534CFF","#BC534CFF")
    
    top_5<- cesa_users_data_()%>%drop_na() %>% 
      select(user_department,username) %>% group_by(user_department) %>% summarise(n=n_distinct(username)) %>% arrange(desc(n))%>% top_n(5)
    
    Data <- cesa_users_data_() %>% 
      merge(top_5,by='user_department', all=TRUE) %>%mutate(user_department=ifelse(is.na(n),'Others',as.character(user_department))) %>% 
      select(user_department,username) %>% group_by(user_department) %>% summarise(n=n_distinct(username)) %>% mutate(prop=round(100*n/sum(n),2)) %>% 
      arrange(desc(user_department)) %>% mutate(lab.ypos = cumsum(prop) - 0.5*prop)
    
    
    ggplot(Data, aes(x = 2, y = prop, fill = user_department)) +
      geom_bar(stat = "identity", color = "white") +
      coord_polar(theta = "y", start = 0)+
      geom_text(aes(y = lab.ypos, label = prop), color = "white")+
      scale_fill_manual(values = mycols) +
      theme_void()+
      xlim(0.5, 2.5)
  })
  
  
  
  
}



# Run the application 
shinyApp(ui = ui, server = server)