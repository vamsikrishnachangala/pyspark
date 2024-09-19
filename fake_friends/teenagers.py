# -*- coding: utf-8 -*-
"""

Extracting Teenagers from Friends dataset using pyspark

Importing Required Libraries
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row

"""Creating a SparkSession"""

spark=SparkSession.builder.appName("SparkSQL").getOrCreate()

"""Loading Dataframe"""

lines=spark.sparkContext.textFile("fakefriends.csv")

"""Defining a mapper function to create row objects"""

def mapper(line):
  fields=line.split(',')
  return Row(ID=int(fields[0]),name=str(fields[1].encode("utf-8")),age=int(fields[2]),numFriends=int(fileds[3]))

people=lines.map(mapper)

"""Creating DataFrame from people row objects and storing in cache to get fast access"""

schemapeople=spark.createDataFrame(people).cache()

"""Creating a Temperory view to prform required operations without disturbing original dataframe"""

schemaPeople.createOrReplaceTempView("people")

"""performing sql operations on created people dataframe"""

teenagers=spark.sql("Select * From people WHERE age >=14 AND age <=19")

"""results will be in teenagers in the form of RDD by calling collect we can change it into python iterable object"""

for teen in teenagers.collect():
  print(teen)

spark.stop()
