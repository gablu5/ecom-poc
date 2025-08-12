import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

# Read sales CSV from raw
sales_df = spark.read.option("header", True).csv("s3://ecom-poc/raw/sales/*.csv")

# Normalize and basic cleaning
sales_df = sales_df.withColumn("qty", sales_df.qty.cast("int")).withColumn("price", sales_df.price.cast("double"))

# Write to bronze as Parquet partitioned by date
sales_df.write.mode("overwrite").partitionBy("date").parquet("s3://ecom-poc/bronze/sales/")

job.commit()
