import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window, to_timestamp

sparkConfig = SparkConf().setAppName("stream-preprocessing")
sparkConfig.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
sparkConfig.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
sparkConfig.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = SparkSession.builder.config(conf=sparkConfig) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("/checkpoint/")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

def _write_streaming(
        df,
        epoch_id
) -> None:
    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', f"jdbc:postgresql://postgres/postgres") \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'streams') \
        .option('user', 'postgres') \
        .option('password', 'pass') \
        .save()

schema =    "type STRING, " + \
            "timestamp STRING, " + \
            "elb_name STRING, " + \
            "client_ip STRING, " + \
            "backend_ip STRING, " + \
            "request_processing_time FLOAT, " + \
            "backend_processing_time FLOAT, " + \
            "response_processing_time FLOAT, " + \
            "elb_status_code STRING, " + \
            "backend_status_code STRING, " + \
            "received_bytes BIGINT, " + \
            "sent_bytes BIGINT, " + \
            "request STRING, " + \
            "user_agent STRING, " + \
            "ssl_cipher STRING, " + \
            "ssl_protocol STRING, " + \
            "target_group_arn STRING"

df = spark.readStream \
        .option("delimiter", " ") \
        .schema(schema) \
        .csv("s3a://ratko-test-bucket/input/aws/")

df = df.withColumn("new_timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))

split_request = split(df['request'], " ")
df = df.withColumn("request_method", split_request.getItem(0))
df = df.withColumn("request_uri", split_request.getItem(1))
df = df.withColumn("request_protocol", split_request.getItem(2))
df.drop("request")

df = df.withWatermark("new_timestamp", "60 seconds")



df = df \
    .writeStream.foreachBatch(_write_streaming) \
    .start() \
    .awaitTermination()

spark.stop()
