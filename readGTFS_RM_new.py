
import pyproj
import pandas as pd
import numpy as np
import datetime

# download gtfs data from https://transitfeeds.com/p/roma-servizi-per-la-mobilita/542

#read calendare_dates
calendar="C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/calendar_dates.txt";
fields = ['service_id','date']
cal= pd.read_csv(calendar)
cal=cal.loc[cal['date'] == 20191125]
print('Calendar',cal.shape)

trips="C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/trips.txt";
fields = ['route_id','service_id','trip_id', 'trip_headsign']
trp= pd.read_csv(trips, usecols=fields)
trp['trip_id']=trp['trip_id'].astype(str)
trp['route_id']=trp['route_id'].astype(str)
trp['trip_headsign']=trp['trip_headsign'].astype(str)
print('trips',trp.shape)

#read routes
routes="C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/routes.txt";
fields = ['route_short_name','route_id']
rt= pd.read_csv(routes, usecols=fields)
rt['route_short_name']=rt['route_short_name'].astype(str)
rt['route_id']=rt['route_id'].astype(str)

# merge with trips by "route_id"
trp=pd.merge(trp, rt, on='route_id', how='left')


#merge calendar_dates and trips
cal_trp=pd.merge(cal, trp, on='service_id', how='left')
del trp
del cal
print('cal_trp', cal_trp.shape)

#read stops
stops="C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/stops.txt";
fields = ['stop_id','stop_name']
stp= pd.read_csv(stops, usecols=fields)
stp['stop_id']=stp['stop_id'].astype(str)
print('Stops', stp.shape)

#read stop_times
stoptimes="C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/stop_times.txt";
fields = ['trip_id','stop_id','stop_sequence','departure_time','shape_dist_traveled']
stpt= pd.read_csv(stoptimes, usecols=fields)
stpt['trip_id']=stpt['trip_id'].astype(str)
stpt['stop_id']=stpt['stop_id'].astype(str)
stpt['departure_time']=stpt['departure_time'].astype(str)
print('StopTimes', stpt.shape) 

#merge merge with cal_trp
cal_trp_stpt=pd.merge(cal_trp[['route_id','trip_id', 'trip_headsign']], stpt[['trip_id', 'stop_id', 'stop_sequence',
                              'departure_time', 'shape_dist_traveled']], on='trip_id', how='inner')
del cal_trp
print(cal_trp_stpt.shape)


#merge with cal_trp_stpt_stp
cal_trp_stpt_stp=pd.merge(cal_trp_stpt[['route_id','trip_id', 'stop_id', 'trip_headsign', 'stop_sequence',
                                        'departure_time', 'shape_dist_traveled']], stp[['stop_id',
                                        'stop_name']], on='stop_id', how='left')
cal_trp_stpt_stp=pd.merge(cal_trp_stpt, stp, on='stop_id', how='left')
del cal_trp_stpt
del stp

#order by trip_id and sequence_id
cal_trp_stpt_stp.sort_values(by=['trip_id','stop_sequence'], inplace=True)
cal_trp_stpt_stp.reset_index(drop=True, inplace=True)
res=cal_trp_stpt_stp.copy()
print('res',res.shape)
res1=cal_trp_stpt_stp.shift(-1, axis = 0)
res1.rename(columns={'route_id':'toroute_id','trip_id':'totrip_id', 'stop_id':'tostop_id',
                     'stop_sequence':'tostop_sequence', 'departure_time':'arrival_time',
                     'shape_dist_traveled':'toshape_dist_traveled',
                     'stop_name':'tostop_name', 'trip_headsign': 'totrip_headsign'},inplace=True)
del cal_trp_stpt_stp
#concate res res1
res2=pd.concat([res, res1],axis=1)
del res
del res1

#Filtering (remove redoundant lines)
result=res2.loc[res2['trip_id'] == res2['totrip_id']]
result = result.drop(['toroute_id', 'tostop_sequence', 'totrip_headsign', 'trip_id', 'totrip_id'] , axis=1)
print(res2.shape, result.shape)
del res2
print(result.dtypes)
print(result.shape)

# rename some columns
result.rename(columns={"trip_headsign": "trip_name",
                       'stop_id':'departure_stop_code',
                       'route_id': 'route_short_name',
                       'tostop_id':'arrival_stop_code',
                       'stop_name':'departure_station',
                       'tostop_name':'arrival_station'}, inplace=True)


# create an INDEX for each stop_code (departure & arrival & route_short_name, & trip_name)
print(result.columns)
result.info()

result = result.assign(id_departure_stop_code=(result['departure_stop_code']).astype('category').cat.codes)
result = result.assign(id_arrival_stop_code=(result['arrival_stop_code']).astype('category').cat.codes)
result = result.assign(id_route_short_name=(result['route_short_name']).astype('category').cat.codes)
result = result.assign(id_trip_name=(result['trip_name']).astype('category').cat.codes)


### # format all hours to 0:24 format
hour = (result["departure_time"].str.split(":", n = 1, expand = True)[0]).astype(int)
hour[hour > 24] = hour-24
max(hour)
hour = hour*3600
min = ((result["departure_time"].str.split(":", n = 2, expand = True)[1]).astype(int))*60
sec = ((result["departure_time"].str.split(":", n = 3, expand = True)[2]).astype(int))
type(hour)
result['departure_time'] = hour+min+sec

# format all hours to 0:24 format
hour = (result["arrival_time"].str.split(":", n = 1, expand = True)[0]).astype(int)
hour[hour > 24] = hour-24
max(hour)
hour = hour*3600
min = ((result["arrival_time"].str.split(":", n = 2, expand = True)[1]).astype(int))*60
sec = ((result["arrival_time"].str.split(":", n = 3, expand = True)[2]).astype(int))
result['arrival_time'] = hour+min+sec

result.dtypes

'''
# transform time to seconds
result["departure_time"] = result["departure_time"].astype(str)
result["arrival_time"] = result["arrival_time"].astype(str)
result["departure_time"] = pd.to_timedelta(result["departure_time"]).astype('timedelta64[s]').astype(int)
result["arrival_time"] = pd.to_timedelta(result["arrival_time"]).astype('timedelta64[s]').astype(int)
'''

# transform time into character
result['departure_time']=result['departure_time'].astype(str)
result['arrival_time']=result['arrival_time'].astype(str)

result['id_departure_stop_code']=result['id_departure_stop_code'].astype(str)
result['id_arrival_stop_code']=result['id_arrival_stop_code'].astype(str)
result['id_route_short_name']=result['id_route_short_name'].astype(str)
result['id_trip_name']=result['id_trip_name'].astype(str)

## order all connections increasing by departure_time ( grouped by departure_stop_code)
result.sort_values(by=['departure_time'], inplace=True)
result.reset_index(drop=True, inplace=True)


result.dtypes
# save .csv file
result.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/timetable_names_python.csv')

# save timetable data to .txt table
timetable_touples = result[['id_departure_stop_code', 'id_arrival_stop_code', 'departure_time', 'arrival_time',
                           'id_route_short_name', 'id_trip_name']]

# save into .csv format
timetable_touples.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/timetable_trips_python_new.txt', sep=',',index=None, header = None)

# save into .txt format (space delimited)
timetable_touples.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/timetable_trips_python.txt', sep=' ', index=None, header = None)

################################################################################
############ find stations (departure/arrival) and departure time ##############
################################################################################

# use 'result' table
AAA = result


## departure station
BBB = AAA[AAA['departure_station'] == 'Villani']
# get unique departure station
departure_unique = BBB['id_departure_stop_code'].unique()[0]

'''
# get uniques names of possible routes
routes_unique = BBB['route_short_name'].unique()
CCC = BBB[BBB['route_short_name'] == "628"]
# CCC = BBB[BBB['route_short_name'] == "665"]
DDD = CCC[CCC['trip_name'] == "VOLPI/FARNESINA"]
id_departure_stop_code = (DDD.drop_duplicates(['id_departure_stop_code'])[['id_departure_stop_code']]).astype(str)
'''

## arrival station
BBB = AAA[AAA['arrival_station'] == 'Magna Grecia/Tuscolo']
BBB = AAA[AAA['arrival_station'] == 'Furio Camillo']
arrival_unique = BBB['id_arrival_stop_code'].unique()
routes_unique = BBB['route_short_name'].unique()
CCC = BBB[BBB['trip_name'] == "MUSE"]
DDD = CCC[CCC['route_short_name'] == "360"]
id_arrival_stop_code = (DDD.drop_duplicates(['id_arrival_stop_code'])[['id_arrival_stop_code']]).astype(str)


## arrival station
BBB = AAA[AAA['arrival_station'] == 'Labicana']
BBB = AAA[AAA['arrival_station'] == 'Pinturicchio']
CCC = BBB[BBB['route_short_name'] == "85"]
DDD = CCC[CCC['trip_name'] == "TERMINI (MA-MB-FS)"]

id_arrival_stop_code = (DDD.drop_duplicates(['id_arrival_stop_code'])[['id_arrival_stop_code']]).astype(str)

XXX = AAA[AAA['id_arrival_stop_code'] == 465]
XXX = AAA[AAA['id_departure_stop_code'] == 569]
XXX = AAA[AAA['id_route_short_name'] == 199]
# departure time ( from 07:00:00)
AAA[AAA['departure_time'] == '25200']


############################
###### load footpaths ######
############################

# foothpaths table is in the same format of the stop_to_stop timetable
# this must to be considered as additional stops
foots = "C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/stoptostop.txt";
footpaths = pd.read_csv(foots)

footpaths.dtypes
footpaths.info()
type(footpaths)

# rename some columns
footpaths.rename(columns={'fromstop_id':'departure_stop_code',
                          'fromstop_name':'departure_station',
                          'tostop_id':'arrival_stop_code',
                          'tostop_name':'arrival_station'}, inplace=True)

# add duration (time in seconds) to each footpath (speed = 1.1 meter/sec)
footpaths['footdur'] = (footpaths['meters']/1.1).astype(int)
# save file
footpaths.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/footpaths.csv')

##############################################################################
##############################################################################
##############################################################################
##############################################################################

import pyproj
import pandas as pd
import numpy as np
import datetime
from array import array

# load timetable
TIMETABLE_names = pd.read_csv("C:/ENEA_CAS_WORK/GTFS_Roma/timetable_names_python.csv")
footpaths = pd.read_csv("C:/ENEA_CAS_WORK/GTFS_Roma/footpaths.csv")
len(footpaths)

TIMETABLE_names.dtypes
footpaths.dtypes
footpaths['departure_stop_code']=footpaths['departure_stop_code'].astype(str)

# get unique "arrival_stop_code" from TIMETABLE_names
TIMETABLE_unique_arrival_name = TIMETABLE_names.drop_duplicates(['arrival_stop_code',
                                                                 'id_arrival_stop_code'])[['arrival_stop_code',
                                                                                           'id_arrival_stop_code']]
# TIMETABLE_unique_arrival_name['id_arrival_stop_code']=TIMETABLE_unique_arrival_name['id_arrival_stop_code'].astype(object)
len(TIMETABLE_unique_arrival_name)

TIMETABLE_unique_departure_name = TIMETABLE_names.drop_duplicates(['departure_stop_code',
                                                                 'id_departure_stop_code'])[['departure_stop_code',
                                                                                           'id_departure_stop_code']]
# TIMETABLE_unique_departure_name['id_departure_stop_code']=TIMETABLE_unique_departure_name['id_departure_stop_code'].astype(object)
len(TIMETABLE_unique_departure_name)

# merge with footpath
arrivals_foot = pd.merge(footpaths, TIMETABLE_unique_arrival_name[['arrival_stop_code', 'id_arrival_stop_code']],
                         on=['arrival_stop_code'], how='left')

len(arrivals_foot)
foots = pd.merge(arrivals_foot, TIMETABLE_unique_departure_name[['departure_stop_code', 'id_departure_stop_code']],
                         on=['departure_stop_code'], how='left')

foots['id_arrival_stop_code'] = foots.id_arrival_stop_code.fillna(0).astype(int)
foots['id_departure_stop_code'] = foots.id_departure_stop_code.fillna(0).astype(int)

foots.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/all_foots.csv')
# save txt file
foots[['footdur',  'id_departure_stop_code', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/all_foots.txt', sep=' ',
                                                          index=None, header = None)

foots[['footdur',  'id_departure_stop_code', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/all_foots_new.csv',
                                                          index=None, header = None)

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
'''
arrivals_foot = pd.merge(footpaths, TIMETABLE_names[['arrival_stop_code', 'arrival_station', 'id_arrival_stop_code']],
                         on=['arrival_stop_code'], how='left')
len(arrivals_foot)

arrivals_foot = footpaths.drop_duplicates(['arrival_stop_code'])[['arrival_stop_code', 'footdur']]
len(arrivals_foot)

# find all footpaths from all arrival_stations
arrivals_foot = footpaths.drop_duplicates(['arrival_stop_code'])[['arrival_stop_code', 'footdur']]
len(arrivals_foot)
arrivals_foot = pd.merge(arrivals_foot, TIMETABLE_names[['arrival_stop_code', 'arrival_station', 'id_arrival_stop_code']],
                         on=['arrival_stop_code'], how='left')
# arrivals_foot.sort_values(by=['id_arrival_stop_code'], inplace=True)
arrivals_foot.drop_duplicates(['arrival_stop_code'], inplace= True)
arrivals_foot['id_arrival_stop_code'] = arrivals_foot.id_arrival_stop_code.fillna(0).astype(int)
len(arrivals_foot)

# save txt file
arrivals_foot[['footdur', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.txt', sep=' ',
                                                          index=None, header = None)

# save .csv file with name of the stops and stop codes
arrivals_foot[['footdur', 'arrival_station', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.csv')
'''
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################



'''
# merge with TIMETABLE when both arrival and departure are present
foots = pd.merge(footpaths, TIMETABLE_names[['departure_stop_code', 'id_departure_stop_code',
                                             'arrival_stop_code', 'id_arrival_stop_code']],
                                on=['departure_stop_code','arrival_stop_code'], how='left')
len(foots)
# foots['id_arrival_stop_code'] = foots.id_arrival_stop_code.fillna(0).astype(int)
foots['id_arrival_stop_code'] = foots.id_arrival_stop_code.fillna(0).astype(int)
foots[['id_arrival_stop_code', 'id_departure_stop_code']] = foots[['id_arrival_stop_code', 'id_departure_stop_code']].fillna(0).astype(int)
# foots = foots.dropna(subset=['id_arrival_stop_code'])
foots = foots.drop_duplicates()
len(foots)

foots.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot_names.csv')
foots[['footdur', 'id_departure_stop_code', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.txt', sep=' ', index=None, header = None)
foots[['footdur', 'id_departure_stop_code', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.csv')
'''

############################
############################

arrivals_foot = pd.merge(footpaths, TIMETABLE_names[['arrival_stop_code', 'id_arrival_stop_code']],
                                on=['arrival_stop_code'], how='left')

arrivals_foot.reset_index(drop=True, inplace=True)
arrivals_foot['id_arrival_stop_code'] = arrivals_foot.id_arrival_stop_code.fillna(0).astype(int)
arrivals_foot = arrivals_foot.dropna(subset=['id_arrival_stop_code'])
arrivals_foot = arrivals_foot.drop_duplicates()

departure_foot = pd.merge(footpaths, TIMETABLE_names[['departure_stop_code', 'id_departure_stop_code']],
                                on=['departure_stop_code'], how='left')

len(arrivals_foot)

arrivals_foot.to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot_names.csv')
arrivals_foot[['footdur', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.txt', sep=' ', index=None, header = None)
arrivals_foot[['footdur', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot.csv')


##########################################################################
##########################################################################
##########################################################################

# source time --> 25200 ("07:00:00")
departure_time = 25200
departure_station = 70588 # Villani  (stop_code)

'''
# find all footpaths from the first departure_station (source)
arrivals_foot_source = footpaths[footpaths['departure_stop_code'] == 70588]
# arrivals_foot_source_name = footpaths[footpaths['departure_station'] == 'Villani']
# get unique arrival_station
# arrivals_foot_source = arrivals_foot_source.drop_duplicates(['arrival_stop_code'])[['arrival_stop_code', 'footdur']]
len(arrivals_foot_source)
type(arrivals_foot_source)
# merge with the timetable to get the ID index of each arrival_station
arrivals_foot_source = pd.merge(arrivals_foot_source, TIMETABLE_names[['arrival_stop_code' , 'id_arrival_stop_code']],
                                on=['arrival_stop_code'], how='left')
arrivals_foot_source.drop_duplicates(['arrival_stop_code'], inplace= True)
len(arrivals_foot_source)
arrivals_foot_source[['footdur', 'id_arrival_stop_code']].to_csv('C:/ENEA_CAS_WORK/GTFS_Roma/arrivals_foot_source.txt', sep=' ', index=None, header = None)
'''

'''
# sort footpaths by departure station

AAA = footpaths
# join the footpath table with the "result" (timetable)
# AAA = pd.merge(result, footpaths.drop('meters', axis=1), on=['departure_stop_code','arrival_stop_code',
#                                       'departure_station', 'arrival_station'], how='left')

# for each record sum the source time of the current "arrival_station" with the duration of the footpath
AAA['footdur'] = AAA.footdur.fillna(0).astype(int)
AAA['arrival_time']=AAA['arrival_time'].astype(int)
AAA['tot_time_footpath'] = AAA['arrival_time'] + AAA['footdur']


AAA = footpaths
# sort footpaths by departure station
BBB = result

AAA.sort_values(by=['departure_station'], inplace=True)
BBB.sort_values(by=['departure_station'], inplace=True)
CCC = (BBB.drop_duplicates(['departure_station', 'arrival_station'])[['departure_station', 'arrival_station']]).astype(str)
'''