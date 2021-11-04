setwd("~/Desktop/st2195_Assignment_3/r_sql")
getwd()

library(DBI)
library(dplyr)

if (file.exists("airline_v2.db"))
  file.remove("airline_v2.db")

conn <- dbConnect(RSQLite::SQLite(), "airline_v2.db")

airports <- read.csv("~/Desktop/SIM/ST2195/dataverse_files/airports.csv", header = TRUE)
carriers <- read.csv("~/Desktop/SIM/ST2195/dataverse_files/carriers.csv", header = TRUE)
planes <- read.csv("~/Desktop/SIM/ST2195/dataverse_files/plane-data.csv", header = TRUE)
dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)

for(i in c(2000:2005)) {
  filename <- paste0("~/Desktop/SIM/ST2195/dataverse_files/", i, ".csv.bz2")
  print(paste("processing:", i))
  ontime <- read.csv(filename, header = TRUE)
  if(i == 2000) {
    dbWriteTable(conn, "ontime", ontime)
  } else {
    dbWriteTable(conn, "ontime", ontime, append = TRUE)
  }
}

gc()

dbListTables(conn)
dbListFields(conn, "ontime")

dbGetQuery(conn,
"SELECT Year, COUNT()
 FROM ontime
 GROUP BY Year"
)

dbGetQuery(conn,
"SELECT 
     COUNT(DISTINCT Tailnum) AS distinct_aircraft,
     COUNT(FlightNUm) AS flights,
     AVG(DepDelay) AS avg_departure_delay,
     MAX(DepDelay) AS max_departure_delay,
     MIN(depDelay) AS min_departure_delay
  FROM ontime
  WHERE Cancelled=0 And Diverted=0"
)

dbGetQuery(conn,
"SELECT
      COUNT(FlightNum) AS flights,
      (Origin || '-' || Dest) as Route
 FROM ontime
 GROUP BY Route
 ORDER BY flights DESC
 LIMIT 10"
)

dbGetQuery(conn,
"SELECT
     carriers.Description AS airline_name,
     ontime.UniqueCarrier AS airline_code,
     COUNT(ontime.FlightNum) AS flights
 FROM ontime LEFT JOIN carriers ON ontime.UniqueCarrier=carriers.Code
 GROUP BY airline_code
 ORDER BY flights DESC
 LIMIT 3"
)

dbGetQuery(conn,
"SELECT
     airports.city,
     COUNT() AS flights
 FROM ontime JOIN airports ON ontime.Dest=airports.iata
 GROUP BY airports.city
 ORDER BY flights DESC
 LIMIT 10"
)

# Q1. Find the plane model with highest average departure delay

q1 <- dbGetQuery(conn,
"SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
 FROM planes JOIN ontime USING (tailnum)
 WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
 GROUP BY model
 ORDER BY avg_delay"
)
print(paste(q1[1, "model"], "has the lowest associated average departure delay."))

write.csv(q1, file = "q1.csv", row.names = F)

(df <- read.csv("q1.csv"))


# Q2. find the city with highest number of inbound flights

q2 <- dbGetQuery(conn,
"SELECT airports.city AS City, count() AS total
 FROM airports JOIN ontime on ontime.dest=airports.iata
 WHERE ontime.Cancelled = 0
 GROUP BY airports.city
 ORDER BY total DESC"
)
print(paste(q2[1, "City"], "has the highest number of inbound flights (excluding cancelled flights."))

write.csv(q2, file = "q2.csv", row.names = F)

(df <- read.csv("q2.csv"))

# Q3. carrier with highest number of cancelled flights

q3 <- dbGetQuery(conn,
"SELECT carriers.Description AS carrier, count() AS total
 FROM carriers join ontime on ontime.UniqueCarrier = carriers.Code
 WHERE ontime.Cancelled = 1
 GROUP BY carriers.Description
 ORDER BY total DESC"
)
print(paste(q3[1, "carrier"], "has the highest number of cancelled flights."))

write.csv(q3, file = "q3.csv", row.names = F)

(df <- read.csv("q3.csv"))


# Q4. carrier with highest ratio of cancelled flights to total flights

q4 <- dbGetQuery(conn,
"SELECT q1.carrier as carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) as ratio 
FROM
(
  SELECT carriers.Description AS carrier, count() AS numerator
  FROM carriers JOIN ontime on ontime.UniqueCarrier = carriers.Code
  WHERE ontime.Cancelled = 1
  GROUP BY carriers.Description
) AS q1 JOIN
(
  SELECT carriers.Description AS carrier, count() AS denominator
  FROM carriers JOIN ontime on ontime.UniqueCarrier = carriers.Code
  GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC"
)
print(paste(q4[1, "carrier"], "highest number of cancelled flights, relative to their number of total flights at: ", round(q4[1, "ratio"],5)))

write.csv(q4, file = "q4.csv", row.names = F)

(df <- read.csv("q4.csv"))

# Simplified Q4 solution
q4_simplified <- dbGetQuery(conn,
"SELECT carriers.Description as carrier, avg(ontime.Cancelled) as cancelled_ratio
 FROM ontime JOIN carriers ON ontime.UniqueCarrier=carriers.Code
 GROUP BY carrier
 ORDER BY cancelled_ratio DESC"
)
print(paste(q4_simplified[1, "carrier"], "highest number of cancelled flights, relative to their number of total flights at: ", round(q4[1, "ratio"],5)))

#q4_simplified <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier"=carriers.Code
  #rename(carrier = Description) %>%
  #group_by(carrier) %>%
  #summarise(ratio = mean(Cancelled, na.rm = TRUE)) %>%
  #arrange(desc(ratio))
#print(head(q4_simplified,1))

# ========== Queries via dplyr ==========

planes_db <- tbl(conn, "planes")
ontime_db <- tbl(conn, "ontime")
carriers_db <- tbl (conn, "carriers")
airports_db <- tbl (conn, "airports")

q1 <- ontime_db %>%
  rename all(tolower) %>%
  inner_join(planes_db, by="tailnum", suffix=c(".ontime", ".planes")) %>%
  filter(Cancelled==0 & Diverted==0 & DepDelay>0) %>%
  group_by(model) %>%
  summarize(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_delay)
print(head(q1, 1))

# ALTERNATIVE Q1 solution
t1 <- ontime_db %>%
  rename(Flight_Year=Year,tailnum=TailNum)

t2 <- planes_db %>%
  rename(plane_year=year)

q1b <- t1 %>%
  inner_join(t2, by="tailnum") %>%
  filter(Cancelled==0 & Diverted==0 & DepDelay>0) %>%
  group_by(model) %>%
  summarize(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_delay)
print(head(q1b, 1))

q2 <- airports_db %>%
  inner_join(airports_db, by= c("Dest"="iata")) %>%
  filter(Cancelled == 0) %>%
  group_by(city) %>%
  summarize(total = n()) %>%
  arrange(desc(total))

print(head(q2, 1))

q3 <- ontime_db %>%
  inner_join(carriers_db, by= c("UniqueCarrier"="Code")) %>%
  filter(Cancelled == 1) %>%
  group_by(Description) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
  
print(head(q3, 1))
  
q4a <- inner_join(ontime_db, carriers_db, by= c("UniqueCarrier"="Code")) %>%
    filter(Cancelled==1) %>%
    group_by(Description) %>%
    summarize(numerator = n()) %>%
    rename(carrier = Description)
      
q4b <- inner_join(ontime_db, carriers_db, by= c("UniqueCarrier"="Code")) %>%
    group_by(Description) %>%         
    summarize(denominator = n()) %>%
    rename(carrier = Description)
    
q4 <- inner_join(q4a, q4b, by= "carrier") %>%
    mutate(numerator = as.double(numerator)) %>%
    mutate(denominator = as.double(denominator)) %>%
    mutate(ratio = numerator/denominator) %>%
    select(carrier, ratio) %>%
    arrange(desc(ratio))
  
print(head(q4, 1))

q4_simplified <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code"))
   rename(carrier = Description) %>%
   group_by(carrier) %>%
   summarize(ratio = mean(Cancelled, na.rm = TRUE)) %>%
   arrange(desc(ratio))
print(head(q4_simplified, 1))

dbDisconnect(conn)

  
                      
      
     

  



 




