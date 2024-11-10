# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Pipeline for velib data snapshot
# MAGIC
# MAGIC We are building here a historical database of all API call that ping station statuses. This database allows plugging a dashboard to vizualize overall bike availability for the Paris bike sharing system called velib.
# MAGIC
# MAGIC We had a historical pipeline build purely on GCP. As the cost started to creep and there were still unresolved technical limitations, I decided to try how this pipeline would look on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Set pipeline environment
# MAGIC
# MAGIC in order to build our pipeline we need we need to:
# MAGIC 1. create a database for storing all tables (rather just having files in a bucket)
# MAGIC 2. access our input data. the historical is on a different account within the same region. the current API calls are stored in our bucket
# MAGIC 3. an ingestion mechanism that would ingest only newly arrived files
# MAGIC 4. an ochestrator to process data from raw (bronze) to aggregated (gold) layer

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os, json, datetime
import pandas as pd
import numpy as np

import dlt

from pyspark.sql.functions import pandas_udf, udf
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType

# COMMAND ----------

# DBTITLE 1,Define input paths for ingestion
inputPath_dbx = spark.conf.get('velib.landing_zone')

# COMMAND ----------

# DBTITLE 1,Specifying Data Schema for JSON
nested_list_schema = """
struct<
     `capacity`:long,
     `lat`: float,
     `lon`: float,
     `name`: string, 
     `rental_methods`: array<string>, 
     `stationCode`: string, 
     `station_id`: long
 >
"""

list_schema = f"""struct<`stations`: array<{nested_list_schema}>>"""

nested_snapshot_schema = """
struct<
     `is_installed`: long,
     `is_renting`: long,
     `is_returning`: long,
     `last_reported`: timestamp, 
     `numBikesAvailable`: long, 
     `numDocksAvailable`:long, 
     `num_bikes_available`: long, 
     `num_docks_available`:long, 
     `stationCode`: string, 
     `num_bikes_available_types`:array<struct<`ebike`:int, `mechanical`: int>>, 
     `station_id`: long
>
"""

snapshot_schema =  f"""struct<`stations`: array<{nested_snapshot_schema}>>"""

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1. Ingestion: Bronze Layer

# COMMAND ----------

# DBTITLE 1,Ingest data from new API call
@dlt.table(
    name='station_list_bronze',
    comment = 'This table ingests stations definition thas is captured live from API call'
)
def ingest_list():
    df_live_list = (
        spark.readStream
            .format('cloudFiles')
            .option("cloudFiles.format", "json")
            .option("cloudFiles.includeExistingFiles", True)
            .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
            .load(f"{inputPath_dbx}/station_list/")
    )
    return df_live_list.select(
        '*',
        F.expr("""
            to_timestamp(
                regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0),
                'yyyy-MM-dd_HH\\'h\\'mm\\'m\\'ss\\'s\\'')
            """).alias('sequence_time'), 
        '_metadata')

@dlt.table(
    name='station_snapshot_bronze',
    comment = 'This table ingests stations snapshot for bike availability thas is captured live from API call'
)
def ingest_snapshot():
    df_live_snapshot = (
        spark.readStream
            .format('cloudFiles')
            .option("cloudFiles.format", "json")
            .option("cloudFiles.includeExistingFiles", True)
            .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
            .load(f"{inputPath_dbx}/station_status/")
    )
    return df_live_snapshot.select(
        '*',
        F.expr("""
            to_timestamp(
                regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0),
                'yyyy-MM-dd_HH\\'h\\'mm\\'m\\'ss\\'s\\'')
            """).alias('snapshot_time'),  
        '_metadata')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Cleaning: Silver Layer

# COMMAND ----------

# DBTITLE 1,UDF to facilitate parsing
@pandas_udf('struct<`mechanical`:int,`ebike`:int>')
def parse_biketype(content:pd.Series) -> pd.DataFrame:
    def parse_array(bike_list):
        parsed_dict = {i:j for json_dict in bike_list for i,j in json_dict.items() if j is not None}
        if parsed_dict =={}:
            parsed_dict = {'mechanical': None, 'ebike': None}
        return parsed_dict
    output = content.apply(parse_array)
    return pd.DataFrame.from_records(output)

# COMMAND ----------

# DBTITLE 1,Build Target Silver Station List
dlt.create_streaming_table(
    name='station_list_silver',
    comment = 'This table parses values and flatten the station_list bronze layer and record the SCD type 2 of all stations',
    # spark_conf = {"<key>" : "<value", "<key" : "<value>"},
    table_properties={"pipelines.autoOptimize.zOrderCols" : "station_id", "delta.enableDeletionVectors": "true"},
    # expect_all = {"<key>" : "<value", "<key" : "<value>"},
    # expect_all_or_drop = {"<key>" : "<value", "<key" : "<value>"},
    # expect_all_or_fail = {"<key>" : "<value", "<key" : "<value>"}
    )

@dlt.view(
    name='station_list_bronze_parsed_view',
    comment = 'This table parses values and flatten the station_list bronze layer'
)
def parse_list():
    df_live_list_silver = (
        dlt.read_stream('station_list_bronze')
            .select('*', F.from_json('data', list_schema).alias('stations'), '_metadata.*').drop('data', '_metadata')
            .withColumn('stations', F.explode('stations.stations'))
            .select('*', 'stations.*').drop('stations')
            .withColumn('geo_point', F.concat(F.lit('POINT('),F.col('lat'), F.lit(','), F.col('lon'), F.lit(')')))
    )
    return df_live_list_silver


dlt.apply_changes(
  target = "station_list_silver",
  source = "station_list_bronze_parsed_view",
  keys = ["station_id"],
  sequence_by = F.col("sequence_time"),
  except_column_list = ["sequence_time"],
  stored_as_scd_type = "2"
)

# COMMAND ----------

# DBTITLE 1,Build Snapshot with append flow
@dlt.table(
    name='station_snapshot_silver',
    comment = 'This table parses values and flatten the station_snapshot bronze layer',
    table_properties={"pipelines.autoOptimize.zOrderCols" : "station_id"}
)
def parse_snapshot():
    # setting for calendar 
    df_dayofweek = (
      spark.createDataFrame([
      ('Sunday', 1), ('Monday', 2), ('Tuesday', 3), ('Wednesday', 4), ('Thursday', 5), ('Friday', 6), ('Saturday', 7),
      ], ['day_string', 'day_number'])
    )
    # parsing station snapshot bronze
    df_live_station_silver = (
        dlt.read_stream('station_snapshot_bronze')
            .select('*', F.from_json('data', snapshot_schema).alias('stations'), '_metadata.*').drop('data', '_metadata')
            .withColumn('stations', F.explode('stations.stations'))
            .select('*','stations.*').drop('stations')
            .withColumn('num_bikes_available_types_parsed', parse_biketype(F.col('num_bikes_available_types')))
            .select('*', 'num_bikes_available_types_parsed.*')
            .withColumn('snapshot_timestamp_year', F.year('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_month', F.month('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_day', F.dayofmonth('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_dayofweek', F.dayofweek('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_hour', F.hour('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_minute', F.minute('snapshot_timestamp'))
            .withColumn('snapshot_timestamp_second', F.second('snapshot_timestamp'))
            .withColumn('origin', F.lit('live'))
            .drop('num_bikes_available_types')
            .drop('num_bikes_available_types_parsed')
    )
    return df_live_station_silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 👉 Next : Build the aggregated Gold Layer
# MAGIC
# MAGIC Keep only non technical fields to the 2 tables and build a view that is the join of the dimension and fact

# COMMAND ----------


