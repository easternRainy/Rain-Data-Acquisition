#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests
import pandas as pd
import time
import os
import sys
from threading import Thread
from datetime import timedelta, date

assert "OutputData02" in os.listdir()

if "OutputData03" not in os.listdir():
    os.mkdir("OutputData03")


# ## Part 3 Do the sum

# In[74]:

def do_sum(df, station_ids):
    values = [df[station_id] for station_id in station_ids]
    
    if -1 in values:
        return -2
    else:
        return sum(values)


def main():
    station_ids = [str(sys.argv[i]) for i in range(1, len(sys.argv))]

    # no command line parameters, default is sum all csv files in OutputData02
    if len(station_ids) == 0:
        station_ids = [file.replace(".csv", "") for file in os.listdir("OutputData02") if file.endswith(".csv")]
        
    print(station_ids)

    for station_id in station_ids:
        assert (f"{station_id}.csv" in os.listdir("OutputData02"))

    df_final = pd.read_csv(os.path.join("OutputData02", f"{station_ids[0]}.csv"))
    for i in range(1,len(station_ids)):
        curr_id = station_ids[i]
        df_tmp = pd.read_csv(os.path.join("OutputData02", f"{curr_id}.csv"))
        df_final[curr_id] = df_tmp[curr_id]

    df_final['Sum'] = df_final.apply(lambda x: do_sum(x, station_ids) , axis=1)
    df_final.to_csv(os.path.join("OutputData03", "Sum.csv"))


# In[89]:
if __name__ == "__main__":
    main()
