# Getting Rain Data from Station Id

## Step 1: Download data from station id
```
python3 01_get_data_by_station_id.py STATION_ID
```

For example, the station id is: SW62701455
```
python3 01_get_data_by_station_id.py SW62701455
```

The output data is stored in ./OutputData and ./OutputData02. The first folder contains data that might have network failure, which is represented by -1. The second folder contains data after processing network failure. However, data in OutputData02 might still has -1, the number of which is much less than in OutputData.

## Step 2: Do sum

```
python3 02_do_sum_by_station_ids.py STATION_ID_00, STATION_ID_01, ...
```

The program will find each OutputData02/STATION_ID.csv. If any one of the ids does not exist, then the program will exit. 

The program will sum the rain data from station ids provided by user and create a new csv file Sum.csv in OutputData03.

You could run this program without command line arguments.
```
python3 02_do_sum_by_station_ids.py
```

The program will add up all csv files listed in OutputData02.

If rain data of any station id at a given time is -1, the corresponding sum value will be -2.