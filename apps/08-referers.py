#!/usr/bin/python

import os
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


def parse_log_row(s) -> Row:
    parsedRow = nginx_log_row(s)
    splitRequest = parsedRow[4].split(' ')
    return Row(
    parsedRow[0],
    parsedRow[1],
    parsedRow[2],
    datetime.strptime(parsedRow[3], "%d/%b/%Y:%H:%M:%S %z"),
    Row(
      splitRequest[0],
      splitRequest[1],
      splitRequest[2]
    ),
    parsedRow[5],
    int(parsedRow[6]),
    #parsedRow[6],
    parsedRow[7],
    parsedRow[8],
    parsedRow[9]
    )

def nginx_log_row(s):
    row = [ ]
    qe = qp = None # quote end character (qe) and quote parts (qp)
    for s in s.replace('\r','').replace('\n','').split(' '):
        if qp:
            qp.append(s)
        elif '' == s: # blanks
            row.append('')
        elif '"' == s[0]: # begin " quote "
            qp = [ s ]
            qe = '"'
        elif '[' == s[0]: # begin [ quote ]
            qp = [ s ]
            qe = ']'
        else:
            row.append(s)

        l = len(s)
        if l and qe == s[-1]: # end quote
            if l == 1 or s[-2] != '\\': # don't end on escaped quotes
                row.append(' '.join(qp)[1:-1].replace('\\'+qe, qe))
                qp = qe = None
    return row

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL - Create DataFrame") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

logsSchema = StructType([
  StructField("remote_addr", StringType(), True),
  StructField("server_name", StringType(), True),
  StructField("remote_user", StringType(), True),
  StructField("time_local", TimestampType(), True),
  StructField("request", StructType([
      StructField("method", StringType(), True),
      StructField("path", StringType(), True),
      StructField("type", StringType(), True)
  ])),
  StructField("status", StringType(), True),
  StructField("body_bytes_sent", IntegerType(), True),
  StructField("http_referer", StringType(), True),
  StructField("http_user_agent", StringType(), True),
  StructField("http_x_forwarded_for", StringType(), True),
])

dfFromTXT = spark.createDataFrame(
  spark.sparkContext.textFile(HDFS_NAMENODE + "/input/test.txt")
  .map(lambda x: parse_log_row(x)),
  logsSchema
)

dfDistinct = dfFromTXT.groupBy('http_referer').count()

dfDistinct \
  .write \
    .mode("overwrite") \
    .option("driver", "org.postgresql.Driver") \
    .jdbc("jdbc:postgresql://postgres/postgres", "public.referers", properties={"user": "postgres", "password": "pass"}) \
