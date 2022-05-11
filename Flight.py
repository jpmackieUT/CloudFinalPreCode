
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




if __name__ == "__main__":
    print("start")

    sc = SparkContext(appName= "TaskExam")

    file1 = "gs://luckybucky/Data/airports.csv.bz2"
    file2 = "gs://luckybucky/Data/flights.csv.bz2"
    lines1 = sc.textFile(file1)
    lines2 = sc.textFile(file1)
    print(lines1.first())
    print(lines2.first())



    #lines1 = lines1.map(lambda x: (x[0], x.split(',')))

    # rdd.map(lambda x: (x[1][x], x[1][y]))
    #   .map(lambda x: (x[0][0], all(i is x[0][1] for i in x[1])))

    # rdd.filter(lambda x: x[1] is True)
    # rdd.reduceByKey(add)
    # rdd.reduceByKey(lambda a,b: a+b)

    # rdd.top(10)
    # rdd.collect()