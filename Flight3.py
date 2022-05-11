
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


def combine(a, b):
    return (a[0]+b[0], a[1]+b[1])

if __name__ == "__main__":

    sc = SparkContext(appName= "TaskExam")

    file ="gs://luckybucky/flights-small.csv"

    lines = sc.textFile(file)
    header = lines.first()
    lines = lines.filter(lambda x: x!= header)

    lines = lines.map(lambda x: x.split(","))

    lines = lines.map(lambda x: (x[0], (1, x[8])))
    lines = lines.filter(lambda x: x[1][1] != "NA")
    lines = lines.map(lambda x: (x[0], (x[1][0], int(x[1][1]))))
    lines = lines.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    lines = lines.map(lambda x: (x[0], x[1][1]/x[1][0]))
    ans = lines.top(7, key=lambda x: 10-int(x[0]))
    for i in ans:
        print(i)
