
import pandas as pd
from datetime import datetime

#path="/Users/gaeval/Documents/sw/gtfs/"
path='C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/'
data='20191213'
dt=data+' '+'00:00:00'
dt_obj = datetime.strptime(dt,'%Y%m%d %H:%M:%S')
date_sec = datetime.timestamp(dt_obj) #date in seconds

#read calendare_dates
calendar=path+"calendar_dates.txt";
fields = ['service_id','date']
dtype_dic= { 'service_id':str, 'date':str}
cal= pd.read_csv(calendar, usecols=fields, dtype = dtype_dic)
cal['date']=cal['date'].astype(str)
cal['service_id']=cal['service_id'].astype(str)
cal=cal.loc[cal['date'] == data]
print('Calendar',cal.shape)

#read trips
trips=path+"trips.txt";
fields = ['route_id','service_id','trip_id']
dtype_dic= {'route_id':str, 'service_id':str, 'trip_id':str}
trp= pd.read_csv(trips, usecols=fields, dtype = dtype_dic)
trp['trip_id']=trp['trip_id'].astype(str)
trp['route_id']=trp['route_id'].astype(str)
trp['service_id']=trp['service_id'].astype(str)
print('trips',trp.shape)

#merge calendar_dates and trips
cal_trp=pd.merge(cal, trp, on='service_id', how='inner')
cal_trp['trip_id']=cal_trp['trip_id'].astype(str)
cal_trp['route_id']=cal_trp['route_id'].astype(str)
cal_trp['service_id']=cal_trp['service_id'].astype(str)
del trp
del cal
print('cal_trp', cal_trp.shape)

#read stops
stops=path+"stops.txt";
fields = ['stop_id','stop_name']
dtype_dic= {'stop_id':str, 'stop_name':str}
stp= pd.read_csv(stops, usecols=fields, dtype = dtype_dic)
stp['stop_id']=stp['stop_id'].astype(str)
stp['stop_name']=stp['stop_name'].astype(str)
print('Stops', stp.shape)

#read stop_times
stoptimes=path+"stop_times.txt";
fields = ['trip_id','stop_id','stop_sequence','arrival_time','departure_time','shape_dist_traveled']
dtype_dic= {'trip_id':str, 'stop_id':str, "stop_sequence":"int64", 'arrival_time':str, 'departure_time':str, "shape_dist_traveled":"float64"}
stpt= pd.read_csv(stoptimes, usecols=fields, dtype = dtype_dic)
stpt['trip_id']=stpt['trip_id'].astype(str)
stpt['stop_id']=stpt['stop_id'].astype(str)
stpt['arrival_time']=stpt['arrival_time'].astype(str)
stpt['departure_time']=stpt['departure_time'].astype(str)
print('StopTimes', stpt.shape)

#merge merge with cal_trp
cal_trp_stpt=pd.merge(cal_trp[['route_id','trip_id']], stpt[['trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time', 'shape_dist_traveled']], on='trip_id', how='inner')
del cal_trp
print(cal_trp_stpt.shape)


#merge merge with cal_trp_stpt_stp
cal_trp_stpt_stp=pd.merge(cal_trp_stpt[['route_id','trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time', 'shape_dist_traveled']], stp[['stop_id', 'stop_name']], on='stop_id', how='left')
cal_trp_stpt_stp=pd.merge(cal_trp_stpt, stp, on='stop_id', how='left')
del cal_trp_stpt
del stp

#order by trip_id and sequence_id
cal_trp_stpt_stp.sort_values(by=['trip_id','stop_sequence'], inplace=True)
cal_trp_stpt_stp.reset_index(drop=True, inplace=True)
res=cal_trp_stpt_stp.copy()
print('res',res.shape)
res1=cal_trp_stpt_stp.shift(-1, axis = 0)
res1.rename(columns={'route_id':'toroute_id','trip_id':'totrip_id', 'stop_id':'tostop_id', 'stop_sequence':'tostop_sequence','arrival_time':'toarrival_time', 'departure_time':'todeparture_time', 'shape_dist_traveled':'toshape_dist_traveled', 'stop_name':'tostop_name'},inplace=True)
del cal_trp_stpt_stp

#concate res res1
res2=pd.concat([res, res1],axis=1)
del res
del res1
#Filtering
result=res2.loc[res2['trip_id'] == res2['totrip_id']]
result = result.drop(['toroute_id', 'tostop_sequence'] , axis=1)

result[['h','m','s']] = result.departure_time.str.split(":",expand=True)
result[['h1','m1','s1']] = result.toarrival_time.str.split(":",expand=True)
result['start_time']=date_sec+3600*(pd.to_numeric(result['h']))+60*(pd.to_numeric(result['m']))+(pd.to_numeric(result['s']))
result['end_time']=date_sec+3600*(pd.to_numeric(result['h1']))+60*(pd.to_numeric(result['m1']))+(pd.to_numeric(result['s1']))
result = result.drop(['m', 'm1', 's', 's1', 'h', 'h1'] , axis=1)
result['shape_dist_traveled'].fillna(1000, inplace = True)
result['toshape_dist_traveled'].fillna(2000, inplace = True)
result['dist']=result['toshape_dist_traveled']-result['shape_dist_traveled']

print(res2.shape, result.shape)

del res2
print(result.dtypes)
result.to_csv(path+data+'.csv')

