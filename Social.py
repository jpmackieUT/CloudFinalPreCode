
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

    file0 = "gs://luckybucky/Data/socialMedia/comment_hasCreator_person.csv.bz2"
    file1 = "gs://luckybucky/Data/socialMedia/comment_replyOf_post.csv.bz2"
    file2 = "gs://luckybucky/Data/socialMedia/person_knows_person.csv.bz2"
    file3 = "gs://luckybucky/Data/socialMedia/person_likes_post.csv.bz2"
    file4 = "gs://luckybucky/Data/socialMedia/post_hasCreator_person.csv.bz2"

    lines0 = sc.textFile(file0)
    lines1 = sc.textFile(file1)
    lines2 = sc.textFile(file2)
    lines3 = sc.textFile(file3)
    lines4 = sc.textFile(file4)

    print(lines0.first())
    print(lines0.top(1))

    print(lines1.first())
    print(lines1.top(1))

    print(lines2.first())
    print(lines2.top(1))

    print(lines3.first())
    print(lines3.top(1))

    print(lines4.first())
    print(lines4.top(1))



    #lines1 = lines1.map(lambda x: (x[0], x.split(',')))

    # rdd.map(lambda x: (x[1][x], x[1][y]))
    #   .map(lambda x: (x[0][0], all(i is x[0][1] for i in x[1])))

    # rdd.filter(lambda x: x[1] is True)
    # rdd.reduceByKey(add)
    # rdd.reduceByKey(lambda a,b: a+b)

    # rdd.top(10)
    # rdd.collect()