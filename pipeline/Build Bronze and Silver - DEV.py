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

from pyspark.sql.functions import pandas_udf, udf
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType

# COMMAND ----------

# DBTITLE 1,Mount the gcs for historian data
try: 
    dbutils.fs.mount(
    dbutils.secrets.get('velib', 'historian_location'), 
    "/mnt/velib-historian",
    extra_configs = {
    "google.cloud.auth.service.account.enable": "true",
    "fs.gs.auth.service.account.email": dbutils.secrets.get('velib', 'service_account'),
    "fs.gs.project.id": dbutils.secrets.get('velib', 'project_id'),
    "fs.gs.auth.service.account.private.key.id": dbutils.secrets.get('velib', 'private_key_id'),  
    "fs.gs.auth.service.account.private.key": dbutils.secrets.get('velib', 'private_key')
  }
)
except:
    print('bucket already mounted')

# COMMAND ----------

# DBTITLE 1,Define input paths for ingestion
spark.conf.set('velib.inputPath_dbx', dbutils.secrets.get('velib', 'database_location'))

inputPath_dbx = spark.conf.get('velib.inputPath_dbx')
inputPath_historian = "/mnt/velib-historian/station-status/2020-09/"

# COMMAND ----------

# DBTITLE 1,Create Database for Storing Data
# MAGIC %sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_velib_basic LOCATION '${velib.inputPath_dbx}/dev_tables_basic/';
# MAGIC USE dev_velib_basic

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Ingestion: Bronze Layer

# COMMAND ----------

# DBTITLE 1,Get historical data and Load in a Table in our Database
df_historian = (
    spark.read.parquet('/mnt/velib-historian/station-status/2020-09/velib_snapshot_2020-09-30-00000-of-00109__00f0a67d-f5fe-41b8-bf07-df01c1d340a1')
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
        .withColumnRenamed('last_updated', 'lastUpdatedOther_station')
        .withColumn('snapshot_timestamp',  F.to_timestamp(F.from_unixtime('snapshot_timestamp')))
        .withColumn('lastUpdatedOther_station',  F.to_timestamp(F.from_unixtime('lastUpdatedOther_station')))
        .withColumn('last_reported', F.to_timestamp(F.from_unixtime('last_reported')))
        .withColumn('origin', F.lit('historian'))
        .drop('key')
)

df_historian.write.mode('overwrite').saveAsTable('historian_bronze')

# COMMAND ----------

# DBTITLE 1,Ingest data from new API call
df_live_list = spark.read.json(f"{inputPath_dbx}/station_list/*").select('*', '_metadata')
df_live_list.write.mode('overwrite').saveAsTable('station_list_bronze')

df_live_snapshot = spark.read.json(f"{inputPath_dbx}/station_status/*").select('*', '_metadata')
df_live_snapshot.write.mode('overwrite').saveAsTable('station_snapshot_bronze')


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE station_list_bronze ZORDER BY snapshot_timestamp;
# MAGIC OPTIMIZE station_snapshot_bronze ZORDER BY snapshot_timestamp;
# MAGIC 
# MAGIC ANALYZE TABLE station_list_bronze COMPUTE STATISTICS FOR COLUMNS snapshot_timestamp, ttl, lastUpdatedOther;
# MAGIC ANALYZE TABLE station_snapshot_bronze COMPUTE STATISTICS FOR COLUMNS snapshot_timestamp, ttl, lastUpdatedOther;

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
df_live_list_silver = (
    spark.read.table('station_list_bronze')
        .select('*', 'data.*', '_metadata.*').drop('data', '_metadata')
        .withColumn('stations', F.explode('stations'))
        .select('*', 'stations.*').drop('stations')
        .withColumn('lastUpdatedOther', F.to_timestamp(F.from_unixtime('lastUpdatedOther')))
        .withColumn('snapshot_timestamp', F.to_timestamp(F.from_unixtime('snapshot_timestamp')))
        .withColumn('geo_point', F.concat(F.lit('POINT('),F.col('lat'), F.lit(','), F.col('lon'), F.lit(')')))
       )

df_live_station_silver = (
    spark.read.table('station_snapshot_bronze')
        .select('*','data.*', '_metadata.*').drop('data', '_metadata')
        .select('*',F.explode('stations').alias('station'))
        .select('*','station.*').drop('station', 'stations')
        .withColumn('num_bikes_available_types_parsed', parse_biketype(F.col('num_bikes_available_types')))
        .select('*', 'num_bikes_available_types_parsed.*')
        .withColumn('mechanical', F.coalesce(F.col('mechanical'), F.lit(0)))
        .withColumn('ebike', F.coalesce(F.col('ebike'), F.lit(0)))
        .withColumn('lastUpdatedOther', F.to_timestamp(F.from_unixtime('lastUpdatedOther')))
        .withColumn('last_reported', F.to_timestamp(F.from_unixtime('last_reported')))
        .withColumn('snapshot_timestamp', F.to_timestamp(F.from_unixtime('snapshot_timestamp')))
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

# change the names of columns that are duplicate except snapshot_time and station_id
col_list = df_live_list_silver.columns
col_snapshot = df_live_station_silver.columns
col_common = list(np.intersect1d(col_list, col_snapshot))
col_common.remove('snapshot_timestamp')
col_common.remove('station_id')

for column in col_common:
    df_live_station_silver = df_live_station_silver.withColumnRenamed(column, f'{column}_station')
    df_live_list_silver = df_live_list_silver.withColumnRenamed(column, f'{column}_list')

df_live_station_silver = df_live_station_silver.join(df_live_list_silver, on = ['snapshot_timestamp','station_id'], how='left')

df_live_station_silver.write.mode('overwrite').saveAsTable('station_list_silver')
df_live_list_silver.write.mode('overwrite').saveAsTable('station_snapshot_silver')

# COMMAND ----------

# DBTITLE 1,Union with historian
df_live = spark.read.table('station_snapshot_silver')
df_historian = spark.read.table('historian_bronze')

merged_df_station = df_live.unionByName(df_historian, allowMissingColumns=True)

merged_df_station.write.mode('overwrite').saveAsTable('station_full_silver')


# COMMAND ----------

# DBTITLE 1,Optimize Silver Table
spark.sql('OPTIMIZE station_list_silver ZORDER BY snapshot_timestamp;')
spark.sql('OPTIMIZE station_snapshot_silver ZORDER BY snapshot_timestamp;')
spark.sql('OPTIMIZE station_full_silver ZORDER BY snapshot_timestamp;')

col_list = spark.sql('DESC TABLE station_list_silver').toPandas()['col_name'].tolist()
col_list.remove('rental_methods')
col_list_string = ', '.join(col_list)
col_snapshot = spark.sql('DESC TABLE station_snapshot_silver').toPandas()['col_name'].tolist()
col_snapshot.remove('rental_methods')
col_snapshot_string = ', '.join(col_snapshot)
col_full = spark.sql('DESC TABLE station_full_silver').toPandas()['col_name'].tolist()
col_full.remove('rental_methods')
col_full_string = ', '.join(col_snapshot)

spark.sql(f'ANALYZE TABLE station_list_silver COMPUTE STATISTICS FOR COLUMNS {col_list_string};')
spark.sql(f'ANALYZE TABLE station_snapshot_silver COMPUTE STATISTICS FOR COLUMNS {col_snapshot_string};')
spark.sql(f'ANALYZE TABLE station_full_silver COMPUTE STATISTICS FOR COLUMNS {col_full_string};')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Build the aggregated Gold Layer

# COMMAND ----------


