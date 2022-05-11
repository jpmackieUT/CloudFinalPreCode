
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


def overUnder(num):
    if(num =="NA"):
        return 0
    if(int(num)>2500):
        return 1
    else:
        return 0

if __name__ == "__main__":

    sc = SparkContext(appName= "TaskExam")

    file ="gs://luckybucky/flights-small.csv"

    lines = sc.textFile(file)
    header = lines.first()
    lines = lines.filter(lambda x: x!= header)

    lines = lines.map(lambda x: x.split(","))

    lines = lines.map(lambda x: ((x[0], x[1]), x[7]))
    print(lines.top(1))
    lines = lines.filter(lambda x: x[1]!="NA")
    lines = lines.map(lambda x: (x[0], int(x[1])))
    print(lines.top(1))
    lines = lines.reduceByKey(add)

    for i in range(1, 8):
        temp = lines.filter(lambda x: x[0][0]==str(i))
        print("day "+str(i)+":")
        print(temp.top(3, key=lambda x: x[1]))
        print()

    # print("top 3")
    # print(lines.top(3, key=lambda x: x[1]))



    #lines1 = lines1.map(lambda x: (x[0], x.split(',')))

    # rdd.map(lambda x: (x[1][x], x[1][y]))
    #   .map(lambda x: (x[0][0], all(i is x[0][1] for i in x[1])))

    # rdd.filter(lambda x: x[1] is True)
    # rdd.reduceByKey(add)
    # rdd.reduceByKey(lambda a,b: a+b)

    # rdd.top(10)
    # rdd.collect()

