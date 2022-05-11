
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
    #['Comment.id|Person.id']

    file1 = "gs://luckybucky/Data/socialMedia/comment_replyOf_post.csv.bz2"
    #['Comment.id|Post.id']

    file2 = "gs://luckybucky/Data/socialMedia/person_knows_person.csv.bz2"
    #['Person.id|Person.id']

    file3 = "gs://luckybucky/Data/socialMedia/person_likes_post.csv.bz2"
    #['Person.id|Post.id|creationDate']

    file4 = "gs://luckybucky/Data/socialMedia/post_hasCreator_person.csv.bz2"
    #['Post.id|Person.id']

    lines0 = sc.textFile(file0)
    lines1 = sc.textFile(file1)
    lines2 = sc.textFile(file2)
    lines3 = sc.textFile(file3)
    lines4 = sc.textFile(file4)

    header0 = lines0.first()
    header1 = lines1.first()
    header2 = lines2.first()
    header3 = lines3.first()
    header4 = lines4.first()
    lines0 = lines0.filter(lambda x: x != header0)
    lines1 = lines1.filter(lambda x: x != header1)
    lines2 = lines2.filter(lambda x: x != header2)
    lines3 = lines3.filter(lambda x: x != header3)
    lines4 = lines4.filter(lambda x: x != header4)

    print(header0)
    print(lines0.top(1))

    print(header1)
    print(lines1.top(1))

    print(header2)
    print(lines2.top(1))

    print(header3)
    print(lines3.top(1))

    print(header4)
    print(lines4.top(1))



    #lines1 = lines1.map(lambda x: (x[0], x.split(',')))

    # rdd.map(lambda x: (x[1][x], x[1][y]))
    #   .map(lambda x: (x[0][0], all(i is x[0][1] for i in x[1])))

    # rdd.filter(lambda x: x[1] is True)
    # rdd.reduceByKey(add)
    # rdd.reduceByKey(lambda a,b: a+b)

    # rdd.top(10)
    # rdd.collect()