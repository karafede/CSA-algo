
import datetime as dt
import pandas as pd
from datetime import datetime
import math
import csv

#####INPUT####################################################################
# departure station
p_stop_id='70588' # Villani
# p_stop_id='72233'
#a_stop_id='75892'
#a_stop_id='70506'
#a_stop_id='79931'
# a_stop_id='73497'
# a_stop_id= "70453" # Magna Grecia/Tuscolo
# a_stop_id= "75944" # Pinturicchio
a_stop_id= "75515" # Furio Camillo


data='20191213'  # this is the timetable
deptime='8:30:00'
footpathspeed=1 #m/s
##########################################################################

now = datetime.now()

##time: from string to second
start_dt=data+' '+deptime
dt_obj = datetime.strptime(start_dt,'%Y%m%d %H:%M:%S')
start_dt_sec = datetime.timestamp(dt_obj)
end_dt_sec= start_dt_sec+(3600*4)
print('start_time',datetime.fromtimestamp(start_dt_sec).strftime("%H:%M:%S"))
print('max_time',datetime.fromtimestamp(end_dt_sec).strftime("%H:%M:%S"))
#path="/Users/gaeval/Documents/sw/gtfs/"
path='C:/ENEA_CAS_WORK/GTFS_Roma/gtfs/'


############loading TIMETABLE  (connections) #############################################
pos_inf = float('inf')     # positive infinity
fields = ['route_id','trip_id','stop_id','start_time','tostop_id','end_time','stop_name', 'tostop_name','dist']
# define a dictionary
dtype_dic= {'route_id':str,'trip_id':str,'stop_id':str,'start_time':"float64",'tostop_id':str,'end_time':"float64", 'stop_name':str, 'tostop_name':str, 'dist':"float64"}
#start = dt.datetime.now()
connections = pd.read_csv(path+data+'.csv', usecols=fields, dtype = dtype_dic)

#column number in dataframe connections
routeid = connections.columns.get_loc("route_id")
tripid = connections.columns.get_loc("trip_id")
stopid = connections.columns.get_loc("stop_id")
tostopid = connections.columns.get_loc("tostop_id")
starttime=connections.columns.get_loc("start_time")
endtime=connections.columns.get_loc("end_time")
stopname=connections.columns.get_loc("stop_name")
tostopname=connections.columns.get_loc("tostop_name")
dist=connections.columns.get_loc("dist")

#From partial dataframe to list 2D
connections1 = connections.loc[(connections['start_time']>=start_dt_sec)]
connections1 = connections1.sort_values(['start_time'])
connections1.sort_values(['start_time'], inplace=True)
# make a list (timetable)
conn = connections1.values.tolist()
del(connections1)
#####################################################################################


####Loading STOPS and create STOPS Dictionary#########################################
stops=path+"stops.txt";
fields = ['stop_id', 'stop_name']
dtype_dic= {'stop_id':str,'stop_name':str}
stp= pd.read_csv(stops, usecols=fields, dtype = dtype_dic)
stop=stp["stop_id"].tolist()
stop__name=stp["stop_name"].tolist()

#list of  infinite
a = [math.inf for x in range(len(stop))]

#create dictionary for stops (ID and names)
stop_dict=dict(zip( stop, a))
stp_name=dict(zip(  stop, stop__name))
#print(stop_dict.get('71109'))
########################################################################################

####Loading foot link #########################################
footpathfile=path+"stoptostop.txt";
fields = ['fromstop_id', 'tostop_id', 'meters']
dtype_dic= {'fromstop_id':str, 'tostop_id':str, 'meters':"float64"}
wl_df= pd.read_csv(footpathfile, usecols=fields, dtype = dtype_dic, sep=',')
wl_df['fromstop_id']=wl_df['fromstop_id'].astype(str)
wl_df['tostop_id']=wl_df['tostop_id'].astype(str)
wl_df['footime']= (wl_df['meters']/footpathspeed).astype(int)
#footpaths=wl_df[wl_df['fromstop_id']=='70588']
#print(footpaths)
########################################################################################

now1 = datetime.now()
print('loading data', now1-now)

#create empty trip and foot dict dictionary
trip = dict()
footp=dict()
#{ keyName1 : value1, keyName2: value2, keyName3: [val1, val2, val3] }
#trip['a'] = 'false'

# setup starting time (datetime)
stop_dict[p_stop_id]=start_dt_sec
#print(stop_dict.get(p_stop_id))

myList = []


for idx, val in enumerate(conn):
    transfer=0
    if stop_dict[a_stop_id] < val[starttime]:
        print('travel time:', (stop_dict.get(a_stop_id) - stop_dict.get(p_stop_id)) / 60,'min')
        break  #algorithm is finished

    if (val[tripid] in trip.keys()):  #####transfer time penalty
        if (trip.get(val[tripid]) == val[stopid]):
            transfer = 0
        else:
            transfer = 60
    else:
        transfer = 60  #####################transfer time penalty

    if (float(stop_dict.get(val[stopid]))+transfer <= (val[starttime])):
                if(val[endtime] < stop_dict.get(val[tostopid])):
                    row = [val[routeid], val[tripid], val[stopid], val[stopname], val[tostopid], val[tostopname],
                               datetime.fromtimestamp(val[starttime]).strftime("%H:%M:%S"),
                               datetime.fromtimestamp(val[endtime]).strftime("%H:%M:%S"), val[starttime], val[endtime],
                               val[dist]]
                    if footp.get(val[stopid])=='footpath':
                        trip[val[tripid]] = val[tostopid]
                        stop_dict[val[tostopid]] = val[endtime]
                        myList.append(row)
                    else:
                        footpaths = wl_df[wl_df['fromstop_id'] == val[stopid]]
                        ### I am taking a footpath following the same path of the bus line
                        for index, rowf in footpaths.iterrows():
                            if rowf['tostop_id'] == val[tostopid]:
                                if stop_dict.get(val[stopid]) + rowf['footime'] < val[endtime]:
                                    stop_dict[rowf['tostop_id']] = stop_dict.get(val[stopid]) + rowf['footime']
                                    trip['footpath'] = rowf['tostop_id']
                                    footp[val[tostopid]]='footpath'
                                    row1 = ['fp', 'footpath', val[stopid], val[stopname], val[tostopid], val[tostopname],
                                            datetime.fromtimestamp(val[starttime]).strftime("%H:%M:%S"),
                                            datetime.fromtimestamp(val[starttime] + rowf['footime'] ).strftime("%H:%M:%S"),
                                            val[starttime], (val[starttime] + rowf['footime']),
                                            rowf['meters']]
                                    myList.append(row1)
                                    #print(row1)
                                else:
                                    trip[val[tripid]] = val[tostopid]
                                    stop_dict[val[tostopid]] = val[endtime]
                                    myList.append(row)
                            ## jump to another station reachable by foot but that was not on the same path of the bus line
                            elif stop_dict.get(rowf['tostop_id']) is not None:
                               #  if stop_dict.get(val[stopid]) + rowf['footime'] < val[endtime]:
                               if (val[starttime] + float(rowf['footime'])) < stop_dict.get(rowf['tostop_id']):
                                    stop_dict[rowf['tostop_id']] = val[starttime] + rowf['footime']
                                    trip['footpath'] = rowf['tostop_id']
                                    footp[val[tostopid]] = 'footpath'
                                    row1 = ['fp', 'footpath', val[stopid], val[stopname], rowf['tostop_id'], stp_name.get(rowf['tostop_id']),
                                            datetime.fromtimestamp(val[starttime]).strftime("%H:%M:%S"),
                                            datetime.fromtimestamp(val[starttime] + rowf['footime']).strftime("%H:%M:%S"),
                                            val[starttime], (val[starttime] + float(rowf['footime'])),
                                            rowf['meters']]
                                    myList.append(row1)



now2 = datetime.now()
print('computation time',now2-now1)
######### build min path
myList.reverse()
myListbis=[]
print(myList)

da=''
for i in range(len(myList)):
       if myList[i][4] == a_stop_id:
           da=myList[i][2]
           myListbis.append(myList[i])
           continue
       if myList[i][4] == da:
           da = myList[i][2]
           myListbis.append(myList[i])
myListbis.reverse()


progressive=0.0
waitingTime=myListbis[0][8]-start_dt_sec #first vehicle stop
vehicle=myListbis[0][0]
change=0
for i in range(len(myListbis)):
    if myListbis[i][0]!=vehicle:
        change+=1
        waitingTime=waitingTime+myListbis[i][8]-myListbis[i-1][9]
        vehicle=myListbis[i][0]
    if myListbis[i][10] is None:
        lung=0
        progressive = progressive + lung
    else:
        lung=myListbis[i][10]
        progressive=progressive+lung
    print(myListbis[i], progressive, change, int(waitingTime/60), int((myListbis[i][9]-start_dt_sec)/60))
print('distance:', progressive)
print('travel time:',(stop_dict.get(a_stop_id) - stop_dict.get(p_stop_id)) / 60,'min')
print('transfer:', change)
print('waiting time:',int(waitingTime/60),'min')
