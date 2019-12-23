
import sys
from array import array
import os
import pyproj
import pandas as pd
import numpy as np
import csv
import datetime
import math
import re

cwd = os.getcwd()

#open csv file
TIMETABLE_names = pd.read_csv("timetable_names_python.csv")
footpaths = pd.read_csv('C:/ENEA_CAS_WORK/GTFS_Roma/all_foots.csv')

# route = pd.read_csv("Villani_Magna_Grecia_Tuscolo_07_00_00_1_route_and_footpaths.csv", header = None)
route = pd.read_csv("Villani_Furio_Camillo_07_00_00_0_route_and_footpaths.csv", header = None)
# route = pd.read_csv("Villani_Pinturicchio_07_00_00_4_route_and_footpaths.csv", header = None)

route.columns = ['id_departure_stop_code',
                 'id_arrival_stop_code',
                 'departure_time',
                 'arrival_time',
                 'foot_duration',
                 'id_route_short_name',
                 'id_trip_name',
                 'alert']



route = pd.merge(route, footpaths[['departure_station', 'arrival_station',
                                   'id_departure_stop_code','id_arrival_stop_code']],
                                       on=['id_departure_stop_code', 'id_arrival_stop_code'], how='left')

route = pd.merge(route, TIMETABLE_names[['trip_name', 'route_short_name', 'id_route_short_name', 'id_trip_name']],
                                       on=['id_route_short_name', 'id_trip_name'], how='left')

# route['id_departure_stop_code'].isin(TIMETABLE_names['id_departure_stop_code'])
# route['id_arrival_stop_code'] = route.id_arrival_stop_code.fillna(0).astype(str)

# convert seconds to string
##...................
# str(datetime.timedelta(seconds=25796))
# '7:09:56'

# pd.to_timedelta(seconds = AAA['departure_time'])

# select unique rows

route = route.drop_duplicates(['id_departure_stop_code','id_arrival_stop_code', 'id_route_short_name', 'id_trip_name',
                           'departure_station', 'arrival_station','departure_time','arrival_time',
                            'foot_duration', 'route_short_name','trip_name', 'alert'])[['id_departure_stop_code',
                            'id_arrival_stop_code', 'id_route_short_name','id_trip_name', 'departure_station',
                            'arrival_station','departure_time', 'arrival_time',
                            'foot_duration', 'route_short_name', 'trip_name', 'alert']]

route['departure_time'] = pd.to_datetime(route["departure_time"], unit='s')
route['arrival_time'] = pd.to_datetime(route["arrival_time"], unit='s')

# print(route.dtypes)
#convert to characters
route["departure_time"] = (route["departure_time"].astype(str)).str[11:]
route["arrival_time"] = (route["arrival_time"].astype(str)).str[11:]

name_departure_station = re.sub('[/," ", :]', '_', np.array(route['departure_station'])[0])
name_arrival_station = re.sub('[/," ", :]', '_', np.array(route['arrival_station'])[len(route)-1])
string_departure_time = re.sub('[/," ", :]', '_',  np.array(route['departure_time'])[0])

# save file to .csv format
route.to_csv('route_with_name_' + name_departure_station + "_" + str(name_arrival_station) + "_"
             + str(string_departure_time) + '.csv')
