
import sys
import re
from operator import add
from pyspark import SparkContext
import numpy as np

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.html
# pyspark.RDD.countByKey
# pyspark.RDD.flatMap
# pyspark.RDD.groupBy
# pyspark.RDD.groupByKey vs reducebykey
# pyspark.RDD.sortBy and pyspark.RDD.sortByKey


def overUnder(num):
    if(num =="NA"):
        return 0
    if(int(num)>2500):
        return 1
    else:
        return 0

if __name__ == "__main__":
    print("start")

    sc = SparkContext(appName= "TaskExam")

    file ="gs://luckybucky/flights-small.csv"

    lines = sc.textFile(file)
    header = lines.first()
    lines = lines.filter(lambda x: x!= header)

    lines = lines.map(lambda x: x.split(","))
    lines = lines.map(lambda x: (x[1], x[6]))
    lines = lines.map(lambda x: (x[0], overUnder(x[1])))
    lines = lines.reduceByKey(add)
    print("top 3")
    print(lines.top(3, key=lambda x: x[1]))



    #lines1 = lines1.map(lambda x: (x[0], x.split(',')))

    # rdd.map(lambda x: (x[1][x], x[1][y]))
    #   .map(lambda x: (x[0][0], all(i is x[0][1] for i in x[1])))

    # rdd.filter(lambda x: x[1] is True)
    # rdd.reduceByKey(add)
    # rdd.reduceByKey(lambda a,b: a+b)

    # rdd.top(10)
    # rdd.collect()



    # Flights/Airports
    #     IATA_CODE is like DFW, CDG, etc

    # gs://luckybucky/Data/airlines.csv.bz2
    #     1 IATA_CODE,
    #     2 AIRLINE

    # gs://luckybucky/Data/airports.csv.bz2
    #     0 IATA_CODE,
    #     1 AIRPORT,
    #     2 CITY,
    #     3 STATE,
    #     4 COUNTRY,
    #     5 LATITUDE,
    #     6 LONGITUDE

    # gs://luckybucky/Data/flights.csv.bz2
    #     0 YEAR,
    #     1 MONTH,
    #     2 DAY,
    #     3 DAY_OF_WEEK,
    #     4 AIRLINE,
    #     5 FLIGHT_NUMBER,
    #     6 TAIL_NUMBER,
    #     7 ORIGIN_AIRPORT,
    #     8 DESTINATION_AIRPORT,
    #     9 SCHEDULED_DEPARTURE,
    #     10 DEPARTURE_TIME,
    #     11 DEPARTURE_DELAY,
    #     12 TAXI_OUT,
    #     13 WHEELS_OFF,
    #     14 SCHEDULED_TIME,
    #     15 ELAPSED_TIME,
    #     16 AIR_TIME,
    #     17 DISTANCE,
    #     18 WHEELS_ON,
    #     19 TAXI_IN,
    #     20 SCHEDULED_ARRIVAL,
    #     21 ARRIVAL_TIME,
    #     22 ARRIVAL_DELAY,
    #     23 DIVERTED,
    #     24 CANCELLED,
    #     25 CANCELLATION_REASON,
    #     26 AIR_SYSTEM_DELAY,
    #     27 SECURITY_DELAY,
    #     28 AIRLINE_DELAY,
    #     29 LATE_AIRCRAFT_DELAY,
    #     30 WEATHER_DELAY