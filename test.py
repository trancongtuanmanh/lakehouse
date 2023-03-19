import pyspark
from delta import *
from pyspark.sql import SparkSession

builder = SparkSession.builder\
    .appName('spark-bigquery-demo') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "gs://lakehouse-prod/test/")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


spark.sql("CREATE TABLE IF NOT EXISTS test.hihi ( id INT, firstName STRING) USING DELTA;").show()