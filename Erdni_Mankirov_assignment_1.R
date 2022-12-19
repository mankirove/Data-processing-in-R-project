### Data Processing in R and Python 2022Z
### Homework Assignment no. 1
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
### Report should include:
### * source() of this file at the begining,
### * reading the data, 
### * attaching the libraries, 
### * execution time measurements (with microbenchmark),
### * and comparing the equivalence of the results,
### * interpretation of queries.
library(sqldf)
library(dplyr)
library(data.table)

Badges <- read.csv("C:\\Users\\user\\Desktop\\Badges.csv")
Posts <- read.csv("C:\\Users\\user\\Desktop\\Posts.csv")
Votes <- read.csv("C:\\Users\\user\\Desktop\\Votes.csv.gz")
Users <- read.csv("C:\\Users\\user\\Desktop\\Users.csv.gz")
Comments <- read.csv("C:\\Users\\user\\Desktop\\Comments.csv.gz")
install.packages("compare")


Everytghingequiv <- function(sqldftest, basetest, dplyrtest, dataTabletest) {
  result <- all(
    compare::compare(sqldftest, basetest, allowAll = TRUE)$result,
    compare::compare(sqldftest, as.data.frame(dplyrtest), allowAll = TRUE)$result,
    compare::compare(sqldftest, as.data.frame(dataTabletest), allowAll = TRUE)$result
  )
  return(result)
}
Everytghingequiv5 <- function(sqldftest,dplyrtest, dataTabletest) {
  result <- all(
    compare::compare(sqldftest, as.data.frame(dataTabletest), allowAll = TRUE)$result,
    compare::compare(sqldftest, as.data.frame(dplyrtest), allowAll = TRUE)$result
   
  )
  return(result)
}
# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

sqldf_1 <- function(Posts){
  sqldf1 <- sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year, COUNT(*) AS TotalNumber
FROM Posts
GROUP BY Year
")
  return(sqldf1)
}

base_1 <- function(Posts){
  years1<-lapply(Posts["CreationDate"], substr, 1, 4)
  count1<-aggregate(Posts$Id, by = years1, FUN = length )
  colnames(count1)<- c("Year","TotalNumber")
  return (count1)
}

dplyr_1 <- function(Posts){
  dployer1 <- Posts %>%
    mutate(Year = substr(CreationDate, 1, 4)) %>%
    group_by(Year) %>%
    summarize(TotalNumber = length(Id)) 
  return(dployer1)
}

data.table_1 <- function(Posts){
  postst <- as.data.table(Posts)
  Years <- postst[, .(Id, Year = format(
    as.POSIXct(CreationDate, format = "%Y-%m-%d"),
    "%Y"))
  ]
  dataTable1 <- Years[, .(TotalNumber = length(Id)),by = Year]
  return(dataTable1)
}
Everytghingequiv(sqldf_1(Posts),base_1(Posts),dplyr_1(Posts),data.table_1(Posts))
microbenchmark::microbenchmark(sqldf = sqldf_1(Posts),base = base_1(Posts),  dplyr = dplyr_1(Posts), dataTable = data.table_1(Posts), times = 1)

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_2 <- function(Users, Posts){
  sqldf2 <- sqldf("SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
FROM Users
JOIN (
SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
) AS Questions
ON Users.Id = Questions.OwnerUserId
GROUP BY Id
ORDER BY TotalViews DESC
LIMIT 10
")
  return(sqldf2)
}

base_2 <- function(Users, Posts){
  tmpt<-Posts[Posts$PostTypeId==1, c('OwnerUserId', 'ViewCount')]
  base2 <-merge(Users, tmpt, by.x = "Id", by.y = "OwnerUserId")
  base2 <-base2[c('Id', 'DisplayName','ViewCount')]
  base2 <- aggregate(ViewCount~Id+DisplayName,base2,FUN = sum)
  base2 <- head(base2[order(base2$ViewCount, decreasing = TRUE),], 10)
  base2 <- setNames(base2, c("Id","DisplayName", "TotalViews"))
  return(base2)
}

dplyr_2 <- function(Users, Posts){
  dplyr2 <- Users %>%
    inner_join(Posts, by = c("Id" = "OwnerUserId")) %>%
    filter(PostTypeId == 1) %>%
    group_by(Id) %>%
    summarize(TotalViews = sum(ViewCount)) %>%
    arrange(desc(TotalViews)) %>%
    slice_head(n = 10) 
    return(dplyr2)
}

data.table_2 <- function(Users, Posts){
  Users <- as.data.table(Users)
  setkey(Users, "Id")
  Posts <- as.data.table(Posts)
  setkey(Posts, OwnerUserId)
  dataTable2 <- head(Users[Posts, nomatch = 0
  ][PostTypeId == 1, .(TotalViews= sum(ViewCount)), by = Id
  ][order(-TotalViews)
  ], 10)
  return(dataTable2)
}


Everytghingequiv(sqldf_2(Users, Posts),base_2(Users, Posts),dplyr_2(Users, Posts),data.table_2(Users, Posts))
microbenchmark::microbenchmark(sqldf = sqldf_2(Users, Posts),base = base_2(Users, Posts),  dplyr = dplyr_2(Users, Posts), dataTable = data.table_2(Users, Posts), times = 1)

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

sqldf_3 <- function(Badges){
  sqldf3 <- sqldf("SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
FROM (
SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
FROM (
SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY Name, Year
) AS BadgesNames
JOIN (
SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY YEAR
) AS BadgesYearly
ON BadgesNames.Year = BadgesYearly.Year
)
GROUP BY Year
")
return(sqldf3)
}

base_3 <- function(Badges){
  years<-lapply(Badges["Date"], substr, 1, 4)
  BadgesYearly<-aggregate(Badges$Id, by = years, FUN = length )
  
  Time <- as.Date(Badges$Date)
  years1<-as.numeric(format(Time,'%Y'))
  BadgesYearly<-BadgesYearly[,c(2,1)]
  colnames(BadgesYearly)<- c("CountTotal","Year")
  BadgesNames<-aggregate(Badges$Id ~ years1 + Badges$Name,data=Badges, FUN = length )
  BadgesNames<-BadgesNames[,c(2,3,1)]
  colnames(BadgesNames)<- c("Name","Count","Year")
  bigjoin <-merge(BadgesNames, BadgesYearly, by.x = "Year", by.y = "Year")
  
  tmp1 <-aggregate(bigjoin$Count*1.0/bigjoin$CountTotal,by = list(bigjoin$Year),FUN = max)
  colnames(tmp1)<- c("Year","MaxPercentage")
  tmp2 <- bigjoin[bigjoin$Year %in% tmp1$Year,c("Year", "Name")]
  base3 <- merge(tmp2, tmp1)
  return(tmp1)
}

dplyr_3 <- function(Badges){
  BadgesYearly <- Badges %>%
    mutate(Year = substr(Date, 1, 4)) %>%
    group_by(Year) %>%
    summarize(CountTotal = length(Id))
  BadgesNames<-Badges %>%
    mutate(Year = substr(Date, 1, 4)) %>%
    group_by(Name,Year) %>%
    summarize(Count = length(Id))
  bigtable <- BadgesYearly %>%
    inner_join(BadgesNames, by = c("Year" = "Year")) %>%
    select(Year, Name, Count, CountTotal)
  
  dplyr3<-bigtable %>%
    group_by(Year) %>%
    summarise(MaxPercentage = max((Count * 1.0) / CountTotal))
  return(dplyr3)
}

data.table_3 <- function(Badges){
  badgest <- as.data.table(Badges)
  Years <- badgest[, .(Id, Year = format(
    as.POSIXct(Date, format = "%Y-%m-%d"),
    "%Y"))]
  BadgesYearly <- Years[, .(CountTotal = length(Id)),by = Year]
  Years2 <- badgest[, .(Id,Name, Year = format( as.POSIXct(Date, format = "%Y-%m-%d"),"%Y"))]
  BadgesName <- Years2[, .(Count = length(Id)),by = list(Name,Year)]
  bigtable1<-BadgesName[BadgesYearly, on = .(Year)]
  dataTable3<-bigtable1[,list(MaxPercentage=max((Count * 1.0) / CountTotal)), by=Year]
  return(dataTable3)
}

Everytghingequiv(sqldf_3(Badges),base_3(Badges),dplyr_3(Badges),data.table_3(Badges))

microbenchmark::microbenchmark(sqldf = sqldf_3(Badges), base = base_3(Badges), dplyr = dplyr_3(Badges), dataTable = data.table_3(Badges), times = 1)

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

sqldf_4 <- function(Comments, Posts, Users){
  sqldf4 <- sqldf("SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
CmtTotScr.CommentsTotalScore
FROM (
SELECT PostId, SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostId
) AS CmtTotScr
JOIN Posts ON Posts.Id = CmtTotScr.PostId
WHERE Posts.PostTypeId=1
) AS PostsBestComments
JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
ORDER BY CommentsTotalScore DESC
LIMIT 10
")
  return(sqldf4)
}

base_4 <- function(Comments, Posts, Users){
  CmtTotScr<-Comments["PostId"]
  CmtTotScr<-aggregate(Comments$Score, by = list(Comments$PostId), FUN = sum )
  colnames(CmtTotScr)<- c("PostId","CommentsTotalScore")
  
  PostsBestComments<-merge(CmtTotScr, Posts[Posts["PostTypeId"] == 1,],by.x = "PostId", by.y = "Id")[c("OwnerUserId", "Title", "CommentCount","ViewCount","CommentsTotalScore")]
  base4<-merge(PostsBestComments,Users,by.x = "OwnerUserId",by.y = "Id")[c("Title", "CommentCount", "ViewCount","CommentsTotalScore","DisplayName","Reputation","Location")]
  base4 <- head(base4[order(base4$CommentsTotalScore, decreasing = TRUE),], 10)
  return(base4)
}

dplyr_4 <- function(Comments, Posts, Users){
  CmtTotScr <- Comments %>%
    group_by(PostId) %>%
    summarize(CommentsTotalScore = sum(Score))
  PostsBestComments<-  CmtTotScr %>%
    inner_join(Posts, by = c("PostId" = "Id")) %>%
    filter(PostTypeId == 1) %>%
    select(OwnerUserId, Title, CommentCount, ViewCount,CommentsTotalScore) 
  
  
  dplyr4 <- PostsBestComments %>%
    inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
    select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)%>%
    arrange(desc(CommentsTotalScore)) %>%
    slice_head(n = 10)
  return(dplyr4)
}

data.table_4 <- function(Comments, Posts, Users){
  postst <- as.data.table(Posts)
  commentst <- as.data.table(Comments)
  userst <- as.data.table(Users)
  CmtTotScr<-commentst[,.(CommentsTotalScore=sum(Score)),by=PostId]
  setkey(CmtTotScr, PostId)
  setkey(postst, Id)
  setkey(userst, Id)
  
  PostsBestComments<-CmtTotScr[postst, nomatch = 0][PostTypeId == 1, .(OwnerUserId, Title, CommentCount, ViewCount,CommentsTotalScore)]
  setkey(PostsBestComments, OwnerUserId)
  dataTable4 <-PostsBestComments[userst, nomatch = 0][,.(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)][order(-CommentsTotalScore)]
  dataTable4 <- head(dataTable4,10)
  return(dataTable4)
}

Everytghingequiv(sqldf_4(Comments, Posts, Users),base_4(Comments, Posts, Users),dplyr_4(Comments, Posts, Users),data.table_4(Comments, Posts, Users))

microbenchmark::microbenchmark(sqldf = sqldf_4(Comments, Posts, Users), base = base_4(Comments, Posts, Users), dplyr = dplyr_4(Comments, Posts, Users), dataTable = data.table_4(Comments, Posts, Users), times = 1)

# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

sqldf_5 <- function(Posts, Votes){
  sqldf5 <- sqldf("SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
FROM Posts
JOIN (
SELECT PostId,
MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes,
MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes,
MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes,
SUM(Total) AS Votes
FROM (
SELECT PostId,
CASE STRFTIME('%Y', CreationDate)
WHEN '2022' THEN 'after'
WHEN '2021' THEN 'during'
WHEN '2020' THEN 'during'
WHEN '2019' THEN 'during'
ELSE 'before'
END VoteDate, COUNT(*) AS Total
FROM votes
WHERE VoteTypeId IN (3, 4, 12)
GROUP BY PostId, VoteDate
) AS VotesDates
GROUP BY VotesDates.PostId
) AS VotesByAge ON Posts.Id = VotesByAge.PostId
WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
ORDER BY DuringCOVIDVotes DESC, votes DESC
LIMIT 20
")
  
  return(sqldf5) 
}

base_5 <- function(Posts, Votes){
  # Input the solution here
  # 
}

dplyr_5 <- function(Posts, Votes){
  VotesDates <- Votes %>%
    filter(VoteTypeId %in% c(3, 4, 12)) %>%
    mutate(VoteDate = substr(CreationDate, 1, 4)) %>%
    mutate(VoteDate = case_when(VoteDate %in% c(2021, 2020,2019) ~ "during", VoteDate %in% c(2022) ~ "after",
                                TRUE ~ "before")) %>%
    count(PostId, VoteDate, name = "Total")
  
  VotesDates2 <- VotesDates %>%
    group_by(PostId) %>%
    summarize(BeforeCOVIDVotes = max(
      case_when(VoteDate == "before" ~ Total, TRUE ~ 0L)),
      DuringCOVIDVotes = max(
        case_when(VoteDate == "during" ~ Total, TRUE ~ 0L)),
      AfterCOVIDVotes = max(
        case_when(VoteDate == "after" ~ Total, TRUE ~ 0L)), Votes= sum(Total))
  
  dplyr5 <- Posts %>%
    inner_join(VotesDates2, by = c("Id" = "PostId")) %>%
    mutate(Date = substr(CreationDate, 1, 10)) %>%
    filter(DuringCOVIDVotes > 0, !Title %in% c("") ) %>%
    select(Title, Date,PostId=Id,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes,Votes) %>%
    arrange(desc(DuringCOVIDVotes),desc(Votes)) %>%
    slice_head(n = 20)
  return(dplyr5)
}

data.table_5 <- function(Posts, Votes){
  votest <- as.data.table(Votes)
  VotesDates <- votest[VoteTypeId %in% c(3, 4, 12), .(PostId, CreationDate)
  ][, .(PostId, VoteDate = fcase(substr(CreationDate, 1, 4)%in% c(2021, 2020,2019), "during", 
                                 substr(CreationDate, 1, 4)%in% c(2022), "after", default = "before" ))
  ][, .(Total = .N), by = .(PostId, VoteDate)
  ]
  
  VotesDates2 <- VotesDates[, .(PostId,
                                BeforeCOVIDVotes = ifelse(VoteDate == "before", Total, 0),
                                DuringCOVIDVotes = ifelse(VoteDate == "during", Total, 0),
                                AfterCOVIDVotes = ifelse(VoteDate == "after", Total, 0),
                                Total)
  ][, .(BeforeCOVIDVotes = max(BeforeCOVIDVotes),
        DuringCOVIDVotes = max(DuringCOVIDVotes),
        AfterCOVIDVotes = max(AfterCOVIDVotes), 
        Votes=sum(Total)),
    by = PostId
  ]
  setkey(VotesDates2, PostId)
  
  postst <- as.data.table(Posts)
  setkey(postst, Id)
  dataTable5 <- head(postst[VotesDates2
  ][DuringCOVIDVotes>0 & !Title %in% "" & !is.na(Title), .(Title, Date = format(as.POSIXct(CreationDate, format = "%Y-%m-%d"),"%Y-%m-%d"),PostId = Id,BeforeCOVIDVotes,DuringCOVIDVotes,AfterCOVIDVotes,Votes)
  ][order(-DuringCOVIDVotes,-Votes)
  ], 20)
  return(dataTable5)
}

Everytghingequiv5(sqldf_5(Posts, Votes),dplyr_5(Posts, Votes),data.table_5(Posts, Votes))

microbenchmark::microbenchmark(sqldf = sqldf_5(Posts, Votes),  dplyr = dplyr_5(Posts, Votes), dataTable = data.table_5(Posts, Votes), times = 1)
