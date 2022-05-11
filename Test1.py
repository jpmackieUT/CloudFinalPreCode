

import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
from itertools import combinations

from pyspark.sql.types import *

from pyspark.sql import functions as f
from pyspark.sql.functions import udf

from pyspark.sql.functions import lit
from pyspark import sql
sqlContext = sql.SparkSession.builder\
        .appName("Word Count") \
        .getOrCreate()


#import pyspark.sql.functions.*

a = [('Chris', 'Budweiser', 15), ('Chris', 'Becks', 5), ('Chris', 'Heineken', 2), ('Bob', 'Becks', 15), ('Bob', 'Budweiser', 10) , ('Bob', 'Heineken', 2) ,  ('Alice', 'Heineken', 8) ] 

rdd = sc.parallelize(a)

df = sqlContext.createDataFrame(rdd, ['drinker', 'beer', 'score'])

sqlContext.registerDataFrameAsTable(df, "drinkers")

print(df.filter('drinker').groupBy('beer').agg({'score': 'sum'}).collect())

print(df.drop('drinker').groupBy('beer').agg({'score': 'sum'}).collect())

print(df.filter('drinker').groupByKey('beer').agg({'score': 'sum'}).collect())

print(df.drop('drinker').groupBy('beer').map({'score': 'sum'}).collect())

print(df.drop('drinker').groupByKey('beer').map(lambda a, b: a+b).collect())

print(df.drop('drinker').groupByKey('beer').map(lambda a, b: a+b).top())

print(df.drop('drinker').groupByKey('beer').reduceByKey(add).collect())

print(df.filter('drinker').groupByKey('beer').reduceByKey(add).collect())