import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# Read commodity price JSON from S3 bronze (written by Lambda)
price_df = spark.read.json("s3://ecom-poc/bronze/commodity_price/")

# Normalize and basic cleaning
price_df = price_df.withColumn("price", price_df.price.cast("double"))

# Write to bronze as Parquet partitioned by date
price_df.write.mode("overwrite").partitionBy("date").parquet("s3://ecom-poc/bronze/commodity_price/")

job.commit()
