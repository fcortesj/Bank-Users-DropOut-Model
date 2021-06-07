from datetime import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Parameters
glue_db = "glue-proyecto-integrador-db-fcj"
glue_tbl = "0_raw"
s3_write_path = "s3://proyecto-integrador-fcj/1-Preprocessed"

#Initialize Parameters
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Log start
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:",dt_start)

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)
data_frame = dynamic_frame_read.toDF()

#Transformations
data_frame_modified = data_frame.drop('rownumber')
data_frame_modified = data_frame_modified.drop('customerid')
data_frame_modified = data_frame_modified.drop('surname')

#Load data
data_frame_modified = data_frame_modified.repartition(1)
dynamic_frame_write = DynamicFrame.fromDF(data_frame_modified, glue_context, "dynamic_frame_write")

#Write into s3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path":s3_write_path,
    },
    format = "csv" # Or Parquet
)

#Log End
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("End time:",dt_end)