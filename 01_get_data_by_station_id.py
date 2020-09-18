#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import time
import os
import sys
from threading import Thread
from datetime import timedelta, date
from multiprocessing import Process

# create project structure
if "OutputData" not in os.listdir():
    os.mkdir("OutputData")

if "OutputData02" not in os.listdir():
    os.mkdir("OutputData02")

if "OutputData03" not in os.listdir():
    os.mkdir("OutputData03")


# In[2]:


class TimeStamp:
        
    def __init__(self, date, time="0800"):
        self.date = date
        self.time = time
        self.tomorrow = self.date + timedelta(1)
        
    def time_stamp(self):
        return self.fmt(self.date)

    def get_tomorrow(self):
        return self.fmt(self.tomorrow)
    
    def fmt(self, date):
        return date.strftime("%Y%m%d"+self.time)


# In[3]:


base_url = "http://bmxx.swj.sh.gov.cn/DataService/JsonService.svc/GetYuLiangTongJi/4/all?"
START_YEAR = 1990
END_YEAR = 2020
START_TIME = TimeStamp(date(1990, 1, 1))
END_TIME = TimeStamp(date(2020, 9, 2))


# In[4]:


# Reference: https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(1990, 1, 1)
end_date = date(2020, 9, 2)

all_timestamps = [single_date.strftime("%Y%m%d")+"0800" for single_date in daterange(start_date, end_date)]
all_endtimestamps = [(single_date+timedelta(1)).strftime("%Y%m%d")+"0800" for single_date in daterange(start_date, end_date)]


# In[5]:


class AQuery:
    '''
    This class does only one query for a particular time and a particular station_id
    '''
    def __init__(self, start_time, end_time, station_id):
        '''
        start_time + 1_day = end_time
        start_time, end_time in the format of YearMonthDayTime
        Time = 0800
        for efficiency purpose, do not check format
        '''
        self.start_time = start_time
        self.end_time = end_time
        self.station_id = station_id
        self.url = f"{base_url}STATIONID={self.station_id}&STARTTIME={self.start_time}&ENDTIME={self.end_time}"
        self.cookies = {}
        
        
        
    def query(self):
        try:
            
            content = requests.get(self.url)
            content = content.json()
            content = content[0]
            rain_value = content.get("RAINVALUE")
            
            if rain_value == None:
                return 0
            else:
                return rain_value
            
        except:
            return -1


# In[6]:


class Worker(Thread):
    '''
    This class works in parallel to get data of some station_id in a year
    '''
    def __init__(self, df, year, station_id, end_date=None):
        Thread.__init__(self)
        self.df = df
        self.year = year
        self.station_id = station_id
        self.start_date = date(self.year, 1, 1)
        if end_date == None:
            self.end_date = date(self.year+1, 1, 1)
        else:
            self.end_date = end_date
        
        
    def run(self):
        '''
        Every worker fill the same DataFrame, but they do not have confilict when wrting to it
        '''
        for single_date in daterange(self.start_date, self.end_date):
            
            curr = TimeStamp(single_date)
            curr_query = AQuery(curr.time_stamp(), curr.get_tomorrow(), self.station_id)
            result = curr_query.query()
            self.df.loc[curr.time_stamp(), self.station_id] = result
            print(f"Unit test: {single_date}, {self.station_id}, {result}")


# In[7]:


def main01(station_id):
    
    df = pd.DataFrame(columns=['Time', 'Time_End', station_id])
    df['Time'] = all_timestamps
    df['Time_End'] = all_endtimestamps
    df = df.set_index('Time')
       
    threads = []
    
    for year in range(START_YEAR, END_YEAR+1):
            
        if year == 2020:
            worker = Worker(df, year, station_id, end_date=date(2020,9,2))
            worker.start()
            threads.append(worker)

        else:
            worker = Worker(df, year, station_id)
            worker.start()
            threads.append(worker)   

        print(f"Unit test: thread created for {year}, {station_id}")
    
    for thread in threads:
        thread.join()
    
    df.to_csv(f"OutputData/{station_id}.csv")


# # Part 2 Process Each -1

# In[10]:


def process_network_failure(station_id):
    df = pd.read_csv(f"OutputData/{station_id}.csv")
    df_fail = df[df[station_id] == -1]
    for i in df_fail.index:
        start_time = str(int(df.iloc[i]["Time"]))
        end_time = str(int(df.iloc[i]["Time_End"]))
        query = AQuery(start_time, end_time, station_id)
        result = query.query()
        df.at[i, station_id] = result
        print(f"Unit test: {start_time}, {station_id}, {result}")
    return df


# In[2]:


def main02(station_id):
    
    df = process_network_failure(station_id)
    df.to_csv(f"OutputData02/{station_id}.csv")


# In[ ]:


def main00(station_id):
    main01(station_id)
    main02(station_id)


def main():
    station_ids = [str(sys.argv[i]) for i in range(1, len(sys.argv))]

    # in args=(station_id, ) the ", " is necessary
    processes = [Process(target=main00, args=(station_id,)) for station_id in station_ids]
    
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()

