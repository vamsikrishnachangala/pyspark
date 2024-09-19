# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 17:18:43 2020

@author: vamsi
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark=SparkSession.builder.appName("mnm_count").getOrCreate()

mnm_df=spark.read.option("header","true").option("inferSchema","true").csv("D:\education\Apache_spark_python_udemy\Learning_Spark_book_practice\mnm_dataset.csv")

mnm_df.show(n=5,truncate=False)
print("Printing MNM by grouping by state and color and agg by count")

count_mnm_df=mnm_df.select("State","Color","Count").groupBy("State","Color").sum("Count").orderBy("sum(Count)",ascending=False)
count_mnm_df.show(n=40,truncate=False)

print("Total no of rows in dataframe"+str(count_mnm_df.count()))

print("Filtering only california state")

ca_mnm_df=mnm_df.select("*").where(mnm_df.State=="CA").groupBy("State","Color").sum("Count").orderBy("sum(Count)",ascending=False)

ca_mnm_df.show(n=50,truncate=False)

