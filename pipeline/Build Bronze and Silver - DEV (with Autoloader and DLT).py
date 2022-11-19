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
# MAGIC ## Set pipeline environment
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

# DBTITLE 1,Mount the gcs for historian data
# try: 
#     dbutils.fs.mount(
#     dbutils.secrets.get('velib', 'historian_location'), 
#     "/mnt/velib-historian",
#     extra_configs = {
#     "google.cloud.auth.service.account.enable": "true",
#     "fs.gs.auth.service.account.email": dbutils.secrets.get('velib', 'service_account'),
#     "fs.gs.project.id": dbutils.secrets.get('velib', 'project_id'),
#     "fs.gs.auth.service.account.private.key.id": dbutils.secrets.get('velib', 'private_key_id'),  
#     "fs.gs.auth.service.account.private.key": dbutils.secrets.get('velib', 'private_key')
#   }
# )
# except:
#     print('bucket already mounted')

# COMMAND ----------

# DBTITLE 1,Define input paths for ingestion
# spark.conf.set('velib.inputPath_dbx', dbutils.secrets.get('velib', 'database_location'))
inputPath_dbx = spark.conf.get('velib.inputPath_dbx')
inputPath_historian = "/mnt/velib-historian/station-status/"
dev_schema = f"{inputPath_dbx}/schema/dlt"

# COMMAND ----------

# DBTITLE 1,Create Database for Storing Data
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_velib_dlt LOCATION '${velib.inputPath_dbx}/dev_tables_dlt/';
# MAGIC USE dev_velib_dlt

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
# MAGIC ## Ingestion: Bronze Layer

# COMMAND ----------

# DBTITLE 1,Get historical data and Load in a Table in our Database
@dlt.table(
    name='historian_bronze',
    comment = 'This table holds historical data thas was captured with the previous architecture',
    table_properties={"pipelines.reset.allowed": "false"}
)
def ingest_historian():
    df_historian = (
        spark.readStream
            .format('cloudFiles')
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.includeExistingFiles", True)
            .option("cloudFiles.schemaLocation", f"{dev_schema}/historian/")
            .option("cloudFiles.useIncrementalListing", True)
            .load(inputPath_historian)
    )
    return df_historian

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
            .option("cloudFiles.schemaLocation", f"{dev_schema}/list/")
            .option("cloudFiles.useIncrementalListing", True)
            .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
            .load(f"{inputPath_dbx}/station_list/")
    )
    return df_live_list.select('*', '_metadata')


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
            .option("cloudFiles.schemaLocation", f"{dev_schema}/snapshot/")
            .option("cloudFiles.useIncrementalListing", True)
            .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
            .load(f"{inputPath_dbx}/station_status/")
    )
    return df_live_snapshot.select('*', '_metadata')

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE station_list_bronze COMPUTE STATISTICS FOR COLUMNS snapshot_timestamp, ttl, lastUpdatedOther;
# MAGIC ANALYZE TABLE station_snapshot_bronze COMPUTE STATISTICS FOR COLUMNS snapshot_timestamp, ttl, lastUpdatedOther;
# MAGIC ANALYZE TABLE historian_bronze COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Cleaning: Silver Layer

# COMMAND ----------

# DBTITLE 1,UDF to facilitate parsing
@pandas_udf('struct<`mechanical`:int,`ebike`:int>')
def parse_biketype(content:pd.Series) -> pd.DataFrame:
    def parse_array(bike_list):
        return {i:j for json_dict in bike_list for i,j in json_dict.items() if j}
    output = content.apply(parse_array)
    return pd.DataFrame.from_records(output)

# COMMAND ----------

# DBTITLE 1,Parse Bronze tables
@dlt.table(
    name='station_list_silver',
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

@dlt.table(
    name='station_snapshot_silver',
    comment = 'This table parses values and flatten the station_snapshot bronze layer'
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
            .withColumn('mechanical', F.coalesce(F.col('mechanical'), F.lit(0)))
            .withColumn('ebike', F.coalesce(F.col('ebike'), F.lit(0)))
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
    df_live_station_silver = (
        df_live_station_silver
            .join(
                df_dayofweek,
                on = F.expr('snapshot_timestamp_dayofweek = day_number'),
                how='left'
            )
        .drop('snapshot_timestamp_dayofweek', 'day_number')
        .withColumnRenamed('day_string','snapshot_timestamp_dayofweek' )
    )
    return df_live_station_silver

@dlt.table(
    name='historian_silver',
    comment = 'This table parses values from historian layer to match live API schema'
)
def parse_historian():
    df_historian_silver = (
        dlt.read_stream('historian_bronze')
            .withColumnRenamed('filename', 'file_name_station')
            .withColumnRenamed('geopoint', 'geo_point')
            .withColumnRenamed('stationCode', 'stationCode_station')
            .withColumnRenamed('snapshot_time', 'snapshot_timestamp')
            .withColumnRenamed('snapshot_time_year', 'snapshot_timestamp_year')
            .withColumnRenamed('snapshot_time_month', 'snapshot_timestamp_month')
            .withColumnRenamed('snapshot_time_day', 'snapshot_timestamp_day')
            .withColumnRenamed('snapshot_time_dayofweek', 'snapshot_timestamp_dayofweek')
            .withColumnRenamed('snapshot_time_hour', 'snapshot_timestamp_hour')
            .withColumnRenamed('snapshot_time_minute', 'snapshot_timestamp_minute')
            .withColumnRenamed('snapshot_time_second', 'snapshot_timestamp_second')
            .withColumnRenamed('last_updated', 'lastUpdatedOther')
            .withColumn('snapshot_timestamp',  F.to_timestamp(F.from_unixtime('snapshot_timestamp')))
            .withColumn('lastUpdatedOther',  F.to_timestamp(F.from_unixtime('lastUpdatedOther')))
            .withColumn('last_reported', F.to_timestamp(F.from_unixtime('last_reported')))
            .withColumn('origin', F.lit('historian'))
            .drop('key')        
    )
    return df_historian_silver

# check stream stream join
@dlt.table(
    name='station_snapshot_joined_silver',
    comment = 'This table augments values from list and snapshot'
)
def join_list_snapshot():
    df_live_station_silver = dlt.read_stream('station_snapshot_silver')
    df_live_list_silver = dlt.read_stream('station_list_silver')
    # change the names of columns that are duplicate except snapshot_time and station_id
    col_list = df_live_list_silver.columns
    col_snapshot = df_live_station_silver.columns
    col_common = list(np.intersect1d(col_list, col_snapshot))
    for column in col_common:
        df_live_station_silver = df_live_station_silver.withColumnRenamed(column, f'{column}_station')
        df_live_list_silver = df_live_list_silver.withColumnRenamed(column, f'{column}_list')
    # joining the 2 tables
    df_live_joined_station_silver = (
        df_live_station_silver.withWatermark("snapshot_timestamp_station", "1 minute")
            .join(
                df_live_list_silver.withWatermark("snapshot_timestamp_list", "1 minute"), 
                on = F.expr("""
                    snapshot_timestamp_station = snapshot_timestamp_list AND
                    station_id_station = station_id_list AND
                    snapshot_timestamp_station >= snapshot_timestamp_list AND
                    snapshot_timestamp_station <= snapshot_timestamp_list + interval 1 minute
                """),
                how='left'
            )
          .withColumnRenamed('snapshot_timestamp_station', 'snapshot_timestamp')
          .withColumnRenamed('station_id_station', 'station_id')
    )
    return df_live_joined_station_silver

# COMMAND ----------

# DBTITLE 1,Union with historian
@dlt.table(
    name='station_full_silver',
    comment = 'This table is the final silver layer for the concenated historical and live API call'
)
def join_list_snapshot():
    df_live = dlt.read_stream('station_snapshot_joined_silver')
    df_historian = dlt.read_stream('historian_silver')

    merged_df_station = df_live.unionByName(df_historian, allowMissingColumns=True)
    
    return merged_df_station

# COMMAND ----------

# DBTITLE 1,Optimize Silver Table
# col_list = spark.sql('DESC TABLE station_list_silver').toPandas()['col_name'].tolist()
# col_list.remove('rental_methods')
# col_list_string = ', '.join(col_list)
# col_snapshot = spark.sql('DESC TABLE station_snapshot_silver').toPandas()['col_name'].tolist()
# col_snapshot.remove('rental_methods')
# col_snapshot_string = ', '.join(col_snapshot)
# col_full = spark.sql('DESC TABLE station_full_silver').toPandas()['col_name'].tolist()
# col_full.remove('rental_methods')
# col_full_string = ', '.join(col_snapshot)

# spark.sql(f'ANALYZE TABLE station_list_silver COMPUTE STATISTICS FOR COLUMNS {col_list_string};')
# spark.sql(f'ANALYZE TABLE station_snapshot_silver COMPUTE STATISTICS FOR COLUMNS {col_snapshot_string};')
# spark.sql(f'ANALYZE TABLE station_full_silver COMPUTE STATISTICS FOR COLUMNS {col_full_string};')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Build the aggregated Gold Layer
