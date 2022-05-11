
import sys
import re
import numpy as np

from operator import add
from pyspark import SparkContext

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.html
# pyspark.RDD.countByKey
# pyspark.RDD.flatMap
# pyspark.RDD.groupBy
# pyspark.RDD.groupByKey vs reducebykey
# pyspark.RDD.sortBy and pyspark.RDD.sortByKey


if __name__ == "__main__":

    sc = SparkContext(appName= "TaskExam")

    file ="gs://luckybucky/flights-small.csv"

    lines = sc.textFile(file)
    header = lines.first()
    lines = lines.filter(lambda x: x!= header)

    lines = lines.map(lambda x: x.split(","))
                                #from, to, day
    lines = lines.map(lambda x: (x[3], x[4], x[0]))

    #from LAX to anywhere but JFK
    fromLAX = lines.filter(lambda x: x[0]=="LAX" and x[1]!="JFK")
    #valid B airport from LAX, day of week
    fromLAX = fromLAX.map(lambda x: (x[1], set(x[2])))
    fromLAX = fromLAX.reduceByKey(lambda a,b: a.union(b))

    #from anywhere but LAX to JFK
    toJFK = lines.filter(lambda x: x[0]!="LAX" and x[1]=="JFK")
    #valid B airport to JFK, day of week
    toJFK = toJFK.map(lambda x: (x[0], set(x[2])))
    toJFK = toJFK.reduceByKey(lambda a,b: a.union(b))

    both = fromLAX.join(toJFK)
    both = both.filter(lambda x: not(len(x[1][1])==1 and x[1][1]==x[1][0]))
    both = both.map(lambda x: x[0])
    all = both.collect()
    for i in all:
        print(i, end=", ")


