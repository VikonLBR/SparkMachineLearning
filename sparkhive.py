from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext, SparkSession
import pandas as pd
import numpy as np



sc = SparkContext('local')
hive_context = HiveContext(sc)
#sqlContext.sql('use userdb')
#sqlContext.sql('select * from hbase_table_1')
testt = hive_context.table("userdb.hivetable").toPandas()
#print testt['id']
#print '**********'
#print testt['name']
#print '**********'
print testt.iloc[0]
print '**********'
print testt.iloc[1]







