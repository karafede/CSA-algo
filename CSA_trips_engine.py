#!/usr/bin/env python3

import sys
from array import array
import os
import pyproj
import pandas as pd
import numpy as np
import csv
import re
import datetime

cwd = os.getcwd()
os.chdir('C:\\ENEA_CAS_WORK\\GTFS_Roma')
input_file = open('timetable_trips_python.txt')

MAX_STATIONS = 200000
MAX_INT = 2**32 - 1

earliest = MAX_INT
departure_station = 569   # Villani
arrival_station = 465   # Magna Grecia/Tuscolo
arrival_station = 7265
# arrival_station = 8165  # Furio Camillo
departure_time = 25200    # ’07:00:00’

# used to populate the valid connections satisfying the CSA algorithm
# build vector S(x) where x are the earliest arrival times
earliest_arrival = array('I', [MAX_INT for _ in range(MAX_STATIONS)])
# used to rebuild the route
in_connection = array('I', [MAX_INT for _ in range(MAX_STATIONS)])
earliest_arrival[departure_station] = departure_time
earliest = MAX_INT

class Connection:
    def __init__(self, line):
        tokens = line.split(" ")
        self.departure_station = int(tokens[0])
        self.arrival_station = int(tokens[1])
        self.departure_timestamp = int(tokens[2])
        self.arrival_timestamp = int(tokens[3])
        self.route_name = int(tokens[4])
        self.trip_name = int(tokens[5])

connections = []
for line in input_file:
    # print(line)
    connections.append(Connection(line.rstrip()))

'''
# first two indexes
i = 130178
i = 131029

# last index
i = 149775
i = 151116

c = connections[i]
'''


for i, c in enumerate(connections):
    if c.departure_timestamp >= earliest_arrival[c.departure_station] \
         and c.arrival_timestamp < earliest_arrival[c.arrival_station]:
                earliest_arrival[c.arrival_station] = c.arrival_timestamp
                in_connection[c.arrival_station] = i
                print(c.departure_station)
                print(c.arrival_station)
                print(c.arrival_timestamp)
                print(i)

                if c.arrival_station == arrival_station:
                    earliest = min(earliest, c.arrival_timestamp)
                    print(earliest)
                    # break
    elif c.arrival_timestamp > earliest:
        break


# initialize empty route list
if in_connection[arrival_station] == MAX_INT:
    print("NO_SOLUTION")
else:
    route = []
    last_connection_index = in_connection[arrival_station]

############################################################################################
# rebuild the route from the last station (index)
while last_connection_index != MAX_INT:
    connection = connections[last_connection_index]
    route.append(connection)
    last_connection_index = in_connection[connection.departure_station]
############################################################################################

for c in reversed(route):
    print("{} {} {} {} {} {}".format(
        c.departure_station,
        c.arrival_station,
        c.departure_timestamp,
        c.arrival_timestamp,
        c.route_name,
        c.trip_name
    ))

