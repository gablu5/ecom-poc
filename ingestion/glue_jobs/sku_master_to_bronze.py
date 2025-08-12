import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# Read SKU master CSV from raw
sku_df = spark.read.option("header", True).csv("s3://ecom-poc/raw/master/sku_master.csv")

# Normalize and basic cleaning
sku_df = sku_df.withColumn("sku", sku_df.sku.cast("string"))

# Write to bronze as Parquet
sku_df.write.mode("overwrite").parquet("s3://ecom-poc/bronze/sku_master/")

job.commit()
