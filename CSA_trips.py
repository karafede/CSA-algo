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
# input_file = open('timetable_trips.txt')

time0 = datetime.datetime.now()
input_file = open('timetable_trips_python.txt')

# load timetable with names, codes and indexes
TIMETABLE_names = pd.read_csv("timetable_names_python.csv")


# lines = [line.rstrip() for line in input_file]
# line = lines[4000]
# tokens = line.split(" ")
# int(tokens[0])
# int(tokens[1])
# int(tokens[2])
# int(tokens[3])

MAX_STATIONS = 200000
MAX_INT = 2**32 - 1

# to do: ADD trip ID and route short name

class Connection:
    def __init__(self, line):
        tokens = line.split(" ")

        lunghezza = len(tokens)
        # print(lunghezza)

        if lunghezza > 5:
            self.departure_station = int(tokens[0])
            self.arrival_station = int(tokens[1])
            self.departure_timestamp = int(tokens[2])
            self.arrival_timestamp = int(tokens[3])
            self.route_name = int(tokens[4])
            self.trip_name = int(tokens[5])


class Timetable:
    # reads all the connections from stdin
    def __init__(self):
        self.connections = []

        for line in input_file:
            if line.rstrip() == "":
                break

            # print(line)
            self.connections.append(Connection(line.rstrip()))

final_arrival_timestamps = []

class CSA:
    def __init__(self):
        self.timetable = Timetable()
        self.in_connection = array('I')
        self.earliest_arrival = array('I')

    def main_loop(self, arrival_station, transfer):
        earliest = MAX_INT

        # for i, c in enumerate(self.timetable.connections):
        for i, c in enumerate(self.timetable.connections):
            if c.departure_timestamp >= self.earliest_arrival[c.departure_station] \
                    and c.arrival_timestamp < self.earliest_arrival[c.arrival_station]:
                self.earliest_arrival[c.arrival_station] = c.arrival_timestamp
                self.in_connection[c.arrival_station] = i

                if c.arrival_station == arrival_station:
                    earliest = min(earliest, c.arrival_timestamp)
            elif c.arrival_timestamp > earliest:
                return

    def print_result(self, arrival_station, name_departure_station, name_arrival_station, string_departure_time,
                              transfer):
        if self.in_connection[arrival_station] == MAX_INT:
            print("NO_SOLUTION")
        else:
            route = []

            # rebuild the route from the arrival station
            last_connection_index = self.in_connection[arrival_station]

            while last_connection_index != MAX_INT:
                connection = self.timetable.connections[last_connection_index]
                route.append(connection)
                last_connection_index = self.in_connection[connection.departure_station]

            # Print it out in the right direction
            final_route_departure = []
            final_route_arrival = []
            final_departure_time = []
            final_arrival_time = []
            final_route = []
            final_trip = []
            for c in reversed(route):
                final_route_departure.append(c.departure_station)
                # print(c.departure_station)
                final_route_arrival.append(c.arrival_station)
                # print(c.arrival_station)
                # print(arrival_station)
                # print(len(route))
                final_departure_time.append(c.departure_timestamp)
                final_arrival_time.append(c.arrival_timestamp)
                final_route.append(c.route_name)
                final_trip.append(c.trip_name)

                print("{} {} {} {} {} {}".format(
                    c.departure_station,
                    c.arrival_station,
                    c.departure_timestamp,
                    c.arrival_timestamp,
                    c.route_name,
                    c.trip_name
                ))

        print("")

        # get number of transfers
        TRANSFERS = len(set(final_route))-1
        print("number of transfers: ", TRANSFERS)
        print("END of elaboration")

        try:
            sys.stdout.flush()
        except BrokenPipeError:
            pass

        # get last arrival time
        EARLIER_ARRIVALS = final_arrival_time[len(final_route) - 1]
        final_arrival_timestamps.append(EARLIER_ARRIVALS)
        # get the minimum arrival timestamp
        print(min(final_arrival_timestamps))

        # convert integers into strings
        final_route_departure_str = list(map(str, final_route_departure))
        final_route_arrival_str = list(map(str, final_route_arrival))
        final_departure_time_str = list(map(str, final_departure_time))
        final_arrival_time_str = list(map(str, final_arrival_time))
        final_route_str = list(map(str, final_route))
        final_trip_str = list(map(str, final_trip))

        print('ARRIVAL_STATION: ', str(arrival_station))

        # save all possible routes (by identified by the ID of the arrival stop code: it can be different for the same
        # station name!)
        '''
        np.savetxt(str(arrival_station) + '_final_route.csv', [p for p in zip(final_route_departure_str,
                                                      final_route_arrival_str,
                                                      final_departure_time_str,
                                                      final_arrival_time,
                                                      final_route_str,
                                                      final_trip_str)], delimiter=',', fmt='%s')
        '''

        if TRANSFERS == transfer:
            np.savetxt(name_departure_station + "_" +str(name_arrival_station) + "_" + str(string_departure_time) +
                       "_" + str(transfer) +'_route.csv',
                       [p for p in zip(final_route_departure_str,
                                                                    final_route_arrival_str,
                                                                    final_departure_time_str,
                                                                    final_arrival_time,
                                                                    final_route_str,
                                                                    final_trip_str)], delimiter=',',fmt='%s')


    def compute(self, departure_station, arrival_station, departure_time, transfer):
        name_departure_station = re.sub('[/," ", :]', '_', departure_station)
        name_arrival_station = re.sub('[/," ", :]', '_', arrival_station)
        string_departure_time = re.sub('[/," ", :]', '_', departure_time)

        input_station = TIMETABLE_names[TIMETABLE_names['departure_station'] == departure_station]
        target_station = TIMETABLE_names[TIMETABLE_names['departure_station'] == arrival_station]
        # transform time string into seconds
        departure_time = sum(x * int(t) for x, t in zip([3600, 60, 1], departure_time.split(":")))

        for departure_station in input_station['id_departure_stop_code'].unique(): departure_station
        # for arrival_station in target_station['id_departure_stop_code'].unique(): arrival_station
        for arrival_station in target_station['id_departure_stop_code'].unique():
            self.in_connection = array('I', [MAX_INT for _ in range(MAX_STATIONS)])
            self.earliest_arrival = array('I', [MAX_INT for _ in range(MAX_STATIONS)])

            self.earliest_arrival[departure_station] = departure_time

            if departure_station <= MAX_STATIONS and arrival_station <= MAX_STATIONS:
                self.main_loop(arrival_station, transfer)

            self.print_result(arrival_station,
                              name_departure_station,
                              name_arrival_station,
                              string_departure_time,
                              transfer)

def main():
    csa = CSA()

    # for line in sys.stdin:
    #     if line.rstrip() == "":
    #         break

        # tokens = line.rstrip().split(" ")
        # tokens = line.rstrip().split(",")
        # print(tokens[0])
        # print(tokens[1])
        # print(tokens[2])
        # print(tokens[3])
        # csa.compute(int(tokens[0]), int(tokens[1]), int(tokens[2]), int(tokens[3]))
        # csa.compute(str(tokens[0]), str(tokens[1]), str(tokens[2]), int(tokens[3]))
    # input variables: Departure station, Arrival station, departure time, number of transfers

    time1 = datetime.datetime.now()
    print(time1 - time0)
    csa.compute("Villani","Magna Grecia/Tuscolo", "07:00:00", 1)
        #csa.compute("Villani","Magna Grecia/Tuscolo", tokens[2], int(tokens[3]))
    # csa.compute("Villani", "Labicana", "07:00:00", 3)

    input_file.close()

if __name__ == '__main__':
    main()


