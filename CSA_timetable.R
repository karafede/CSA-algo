
setwd("C:/ENEA_CAS_WORK/GTFS_Roma")
# download gtfs data from here: https://transitfeeds.com/p/roma-servizi-per-la-mobilita/542
# load gtfs data for 14 November 2019

WD <- "C:/ENEA_CAS_WORK/GTFS_Roma/gtfs"



library(readr)
library(dplyr)
library(lubridate)
library(stringr)
library(gtfsrouter)
library(chron)


calendar <- read.csv(paste0(WD,"/calendar_dates.txt"))
# get calendar date of 19/11/2019
calendar <- calendar[calendar$date =="20191125", ]
str(calendar)
trips <- read.csv(paste0(WD,"/trips.txt"))
trips$trip_id <- as.character(trips$trip_id)
str(trips)

# merge calendar and trips by "service_id
trips <- calendar %>%
  left_join(trips, by = "service_id")

routes <- read.csv(paste0(WD,"/routes.txt"))
stops <- read.csv(paste0(WD,"/stops.txt"))
stops$stop_id <- as.character(stops$stop_id)
stop_times <- read.csv(paste0(WD,"/stop_times.txt"))
stop_times$stop_id <- as.character(stop_times$stop_id)
stop_times$trip_id <- as.character(stop_times$trip_id)

str(stops)
str(stop_times)
stop_times$arrival_time <- as.character(stop_times$arrival_time)
stop_times$departure_time <- as.character(stop_times$departure_time)

# join routes and stops names to stop_times
stop_times <- stop_times %>%
  left_join(trips) %>%
  left_join(routes) %>%
  left_join(stops) %>%
  select(route_id,
         route_short_name,
         trip_id,
         trip_headsign,
         stop_code,
         stop_name,
         service_id,
         arrival_time,
         departure_time,
         direction_id,
         shape_id,
         stop_sequence)


# remove NA lines in from the column "rout_id"
stop_times <- stop_times[!is.na(stop_times$route_id) ,]


# transform characters into numbers
stop_times <- stop_times %>%
  # hours
  mutate(
    arrival_time_hour = ifelse(
      as.integer(substr(arrival_time, 1, 2)) < 24,
      as.integer(substr(arrival_time, 1, 2)),
      as.integer(substr(arrival_time, 1, 2)) - 24),
    departure_time_hour = ifelse(
      as.integer(substr(departure_time, 1, 2)) < 24,
      as.integer(substr(departure_time, 1, 2)),
      as.integer(substr(departure_time, 1, 2)) -24),
    # minutes
    arrival_time_min = as.integer(substr(arrival_time, 4, 5)),
    departure_time_min = as.integer(substr(departure_time, 4, 5)),
    # seconds
    arrival_time_sec = as.integer(substr(arrival_time, 7, 8)),
    departure_time_sec = as.integer(substr(departure_time, 7, 8))
  )


stop_times$departure_time <- sprintf("%02d:%02d:%02d",
                                     stop_times$departure_time_hour,
                                     stop_times$departure_time_min,
                                     stop_times$departure_time_sec)
stop_times$arrival_time <- sprintf("%02d:%02d:%02d",
                                   stop_times$arrival_time_hour,
                                   stop_times$arrival_time_min,
                                   stop_times$arrival_time_sec)

TIMETABLE_FK <- stop_times[c("route_id", "trip_headsign", "route_short_name", "arrival_time", "departure_time", "stop_name",
                            "stop_code",  "stop_sequence")]
TIMETABLE_FK$arrival_time <- as.character(TIMETABLE_FK$arrival_time)
TIMETABLE_FK$departure_time <- as.character(TIMETABLE_FK$departure_time)
TIMETABLE_FK$stop_name <- as.character(TIMETABLE_FK$stop_name)
TIMETABLE_FK$stop_code <- as.character(TIMETABLE_FK$stop_code)
TIMETABLE_FK$route_id <- as.character(TIMETABLE_FK$route_id)
TIMETABLE_FK$trip_headsign <- as.character(TIMETABLE_FK$trip_headsign)
TIMETABLE_FK$route_short_name <- as.character(TIMETABLE_FK$route_short_name)
str(TIMETABLE_FK)


# for each trip_id, put 2 consecutive sequence into one line in the TIMETABLE_FK table
TIMETABLE_FK <- TIMETABLE_FK %>%
  group_by(route_id) %>%
  mutate(arrival_time_bis = c(arrival_time[2:length(arrival_time)], "to_remove")) %>%
  mutate(arrival_station = c(stop_name[2:length(stop_name)], "to_remove")) %>%
  mutate(arrival_stop_code = c(stop_code[2:length(stop_code)], "to_remove"))


# remove all the rows containing "to_remove"
TIMETABLE_FK <- TIMETABLE_FK[!grepl("to_remove", TIMETABLE_FK$arrival_time_bis), ]

# rename columns
names(TIMETABLE_FK)[names(TIMETABLE_FK) == "stop_name"] <- "departure_station"
names(TIMETABLE_FK)[names(TIMETABLE_FK) == "stop_code"] <- "departure_stop_code"
TIMETABLE_FK <- TIMETABLE_FK[ , -which(names(TIMETABLE_FK) %in% c("arrival_time","stop_sequence"))]
names(TIMETABLE_FK)[names(TIMETABLE_FK) == "arrival_time_bis"] <- "arrival_time"
names(TIMETABLE_FK)[names(TIMETABLE_FK) == "trip_headsign"] <- "trip_name"

# order columns
TIMETABLE_FK <- TIMETABLE_FK %>%
  dplyr::select(route_id,
                trip_name,
                route_short_name,
                departure_time,
                arrival_time,
                departure_station,
                arrival_station,
                departure_stop_code,
                arrival_stop_code)

# filters times
str(TIMETABLE_FK)

# make a time (filter times larger than a certain hour)
TIMETABLE_FK <- as.data.frame(TIMETABLE_FK)


# departure station
dep_station <- "Furio Camillo"
dep_station <- "Valle Aurelia"
dep_station <- "Villani"

# arrival station
arr_station <- "Termini"
arr_station <- "Mancini"
arr_station <- "Magna Grecia/Tuscolo"
arr_station <- "Labicana"
# arr_station <- "Colosseo"


### order all connections increasing by departure time
TIMETABLE_FK <- TIMETABLE_FK %>%
  dplyr::group_by(departure_stop_code) %>%
  arrange(departure_time)

to_subtract <- as.numeric(as.POSIXct("2019-11-22 01:00:00", tz = "GMT", origin="1970-01-01")) # 22nd November 2019
as.numeric(as.POSIXct(paste0("2019-11-22", " ", TIMETABLE_FK$departure_time), tz = "GMT", origin="1970-01-01")) - to_subtract

# "07:00:00"
as.numeric(as.POSIXct(paste0("2019-11-22", " ", "07:00:00"), tz = "GMT", origin="1970-01-01")) - to_subtract


df <- '24:00:00'
df <- "08:06:00"
df <- "07:00:00"
# df <- "07:09:06"
# df <- "14:30:00"
# df <- "7:31:02"
# df <- start_time

as.numeric(hms(df))
# [1] 25746  ("07:09:06")
#as.numeric(as.POSIXct(paste0("2019-11-22", " ", df), tz = "GMT", origin="1970-01-01")) - to_subtract
# seconds in a day ---> 60*60*24 = 86400

# convert back seconds to time
td <- seconds_to_period(86400)
td <- seconds_to_period((26474))
td <- seconds_to_period((25746))
109800
td <- seconds_to_period((109800))

# sprintf('%02d %02d:%02d:%02d', day(td), td@hour, minute(td), second(td))
sprintf('%02d:%02d:%02d', td@hour, minute(td), second(td))


# create an ID for each stop_code (departure & arrival & route_short_name, & trip_name)
TIMETABLE_FK <- transform(TIMETABLE_FK, id_departure_stop_code = as.numeric(factor(departure_stop_code)))
TIMETABLE_FK <- transform(TIMETABLE_FK, id_arrival_stop_code = as.numeric(factor(arrival_stop_code)))

TIMETABLE_FK <- transform(TIMETABLE_FK, id_route_short_name = as.numeric(factor(route_short_name)))
TIMETABLE_FK <- transform(TIMETABLE_FK, id_trip_name = as.numeric(factor(trip_name)))

TIMETABLE_touple <- TIMETABLE_FK %>%
  dplyr::select(id_departure_stop_code,
                id_arrival_stop_code,
                departure_time,
                arrival_time)

TIMETABLE_touple_trips <- TIMETABLE_FK %>%
  dplyr::select(id_departure_stop_code,
                id_arrival_stop_code,
                departure_time,
                arrival_time,
                id_route_short_name,
                id_trip_name)


# make time as numeric
TIMETABLE_touple$departure_time <- as.numeric(hms(TIMETABLE_touple$departure_time))
TIMETABLE_touple$arrival_time <- as.numeric(hms(TIMETABLE_touple$arrival_time))

TIMETABLE_touple_trips$departure_time <- as.numeric(hms(TIMETABLE_touple_trips$departure_time))
TIMETABLE_touple_trips$arrival_time <- as.numeric(hms(TIMETABLE_touple_trips$arrival_time))

# save timetable
write.csv(TIMETABLE_FK, "TIMETABLE_FK_names.csv")
write.table(TIMETABLE_touple, "timetable.txt", quote = FALSE, row.names = FALSE, col.names = F)
write.table(TIMETABLE_touple_trips, "timetable_trips.txt", quote = FALSE, row.names = FALSE, col.names = F)


#########################################################################################################
#########################################################################################################
#########################################################################################################
