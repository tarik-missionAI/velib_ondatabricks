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
# MAGIC ## ⚙️ 0. Pipeline environment
# MAGIC
# MAGIC in order to build our pipeline we need we need to:
# MAGIC 1. create a database for storing all tables (rather just having files in a bucket)
# MAGIC 2. access our input data. the historical is on a different account within the same region. the current API calls are stored in our bucket
# MAGIC 3. an ingestion mechanism that would ingest only newly arrived files
# MAGIC 4. an ochestrator to process data from raw (bronze) to aggregated (gold) layer

# COMMAND ----------

# for row in spark.sql(f"SHOW TABLES IN velib.`velib-{dbutils.widgets.get('environment')}`").collect():
#   spark.sql(f"DROP TABLE velib.`velib-{dbutils.widgets.get('environment')}`.{row.tableName}")

# dbutils.fs.rm(f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/", recurse=True)
# dbutils.fs.rm(f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/schema/", recurse=True)


# COMMAND ----------

# DBTITLE 1,Import Libraries
import os, json, datetime
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

from pyspark.sql.functions import pandas_udf, udf
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType
from pyspark.sql.window import Window

# Set spark config for the evolving the schema in the merge 
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "True")


spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")



# COMMAND ----------

# DBTITLE 1,Define input paths for ingestion
if dbutils.widgets.get('environment') is not None:
    inputPath_dbx = f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/landing_zone"
else:
    # Provide a default value or handle the case when the widget is not set
    # For example:
    inputPath_dbx = "/Volumes/velib/velib-dev/landing_zone"

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

# DBTITLE 1,Create Tables with parameters and comments
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_list_bronze (data string)
# MAGIC COMMENT "This table ingests stations definition thas is captured live from API call";
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_list_silver (station_id bigint, dedup_hash string)  
# MAGIC COMMENT "This table parses values and flatten the station_list bronze layer"
# MAGIC CLUSTER BY (station_id);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_list_gold (station_id bigint, merge_key STRING)
# MAGIC COMMENT "This table gets the station_list bronze layer and record the SCD type 2 of all stations"
# MAGIC CLUSTER BY (station_id);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_snapshot_bronze (data string)
# MAGIC COMMENT "This table ingests stations snapshot for bike availability thas is captured live from API call";
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_snapshot_silver (station_id bigint)
# MAGIC COMMENT "This table parses values and flatten the station_snapshot bronze layer"
# MAGIC CLUSTER BY (station_id);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS velib.`velib-${environment}`.station_snapshot_gold (station_id bigint)
# MAGIC COMMENT "This table gets the station_snapshot silver layer qnd retains only non technical columns"
# MAGIC CLUSTER BY (station_id);

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 🥉1. Ingestion: Bronze Layer

# COMMAND ----------

# DBTITLE 1,Define autoloader options that are common
autoloader_options = {
    "cloudFiles.format": "json",
    "cloudFiles.includeExistingFiles": True,
    "cloudFiles.region": "eu-west-1",
    "cloudFiles.useNotifications": True,
    "cloudFiles.maxFilesPerTrigger": 10000,
    "cloudFiles.backfillInterval": "1 month",
    "cloudFiles.fetchParallelism": 4,
    "cloudFiles.resourceTag.project": "velib"
}


# COMMAND ----------

# DBTITLE 1,Ingest data from new API call
# Ingest files from station list
df_live_list = (
    spark.readStream
        .format('cloudFiles')
        .options(**autoloader_options)
        .option("cloudFiles.schemaLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/schema/station_list_schema/")
        .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
        .load(f"{inputPath_dbx}/station_list/")
)

query_station_list_bronze = (
    df_live_list
    .select(
        '*',
        F.expr("""
            to_timestamp(
                regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0),
                'yyyy-MM-dd_HH\\'h\\'mm\\'m\\'ss\\'s\\'')
            """).alias('sequence_time'), 
        '_metadata')
    .withColumn('snapshot_timestamp', F.coalesce('snapshot_timestamp', 'sequence_time'))
    .writeStream
    .queryName("station_list_bronze")
    .option("mergeSchema", "true")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_list_bronze")
    .trigger(availableNow = True)
    .toTable(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_list_bronze")
)

query_station_list_bronze.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Ingest data from new API call part II
# Ingest files from station status
df_live_snapshot = (
        spark.readStream
            .format('cloudFiles')
            .options(**autoloader_options)
            .option("cloudFiles.schemaLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/schema/station_status_schema/")
            .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
            .load(f"{inputPath_dbx}/station_status/")
    )

query_station_snapshot_bronze = (
    df_live_snapshot
    .select(
            '*',
            F.expr("""
                to_timestamp(
                    regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0),
                    'yyyy-MM-dd_HH\\'h\\'mm\\'m\\'ss\\'s\\'')
                """).alias('sequence_time'),  
            '_metadata')
    .writeStream
    .queryName("station_snapshot_bronze")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_snapshot_bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow = True)
    .toTable(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_snapshot_bronze")
      
)

query_station_snapshot_bronze.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🥈2. Cleaning: Silver Layer

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
df_live_list_silver = (
        spark.readStream.table(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_list_bronze")
            .select('*', F.from_json('data', list_schema).alias('stations'), '_metadata.*').drop('data', '_metadata')
            .withColumn('stations', F.explode('stations.stations'))
            .select('*', 'stations.*').drop('stations')
            .withColumn('geo_point', F.concat(F.lit('POINT('),F.col('lat'), F.lit(','), F.col('lon'), F.lit(')')))
            .withColumn('dedup_hash', F.md5(F.concat_ws(
                ",", 
                "capacity", 
                "lat", 
                "lon", 
                "name", 
                F.concat_ws(",", "rental_methods"),
                "stationCode"
                )))
)

query_station_list_silver = (
    df_live_list_silver
    .writeStream
    .queryName("station_list_silver")
    .option("mergeSchema", "true")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_list_silver")
    .trigger(availableNow = True)
    .toTable(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_list_silver")
)

query_station_list_silver.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Build Snapshot with append flow
# setting for calendar 
df_dayofweek = (
    spark.createDataFrame([
    ('Sunday', 1), ('Monday', 2), ('Tuesday', 3), ('Wednesday', 4), ('Thursday', 5), ('Friday', 6), ('Saturday', 7),
    ], ['snapshot_timestamp_dayofweek', 'day_number'])
)
# parsing station snapshot bronze
df_live_station_silver = (
    spark.readStream.table(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_snapshot_bronze")
        .select('*', F.from_json('data', snapshot_schema).alias('stations'), '_metadata.*').drop('data', '_metadata')
        .withColumn('stations', F.explode('stations.stations'))
        .select('*','stations.*').drop('stations')
        .withColumn('num_bikes_available_types_parsed', parse_biketype(F.col('num_bikes_available_types')))
        .select('*', 'num_bikes_available_types_parsed.*')
        .withColumns({
            'snapshot_timestamp_year': F.year('snapshot_timestamp'),
            'snapshot_timestamp_month': F.month('snapshot_timestamp'),
            'snapshot_timestamp_day': F.dayofmonth('snapshot_timestamp'),
            'snapshot_timestamp_dayofweek_int': F.dayofweek('snapshot_timestamp'),
            'snapshot_timestamp_hour': F.hour('snapshot_timestamp'),
            'snapshot_timestamp_minute': F.minute('snapshot_timestamp'),
            'snapshot_timestamp_second': F.second('snapshot_timestamp')
            })
        .join(
            df_dayofweek,
            on = F.expr('snapshot_timestamp_dayofweek_int = day_number'),
            how='left'
        )
        .withColumnRenamed('day_string','snapshot_timestamp_dayofweek' )
        .drop(*['day_number', 'num_bikes_available_types', 'num_bikes_available_types_parsed'])
)

query_station_snapshot_silver = (
    df_live_station_silver
    .writeStream
    .queryName("station_snapshot_silver")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_snapshot_silver")
    .option("mergeSchema", "true") 
    .trigger(availableNow = True)
    .toTable(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_snapshot_silver")
)

query_station_snapshot_silver.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 👉 Next : Build the aggregated Gold Layer
# MAGIC
# MAGIC Keep only non technical fields to the 2 tables and build a view that is the join of the dimension and fact

# COMMAND ----------

# DBTITLE 0,Build the SCD type 2 on the station list
# Read the incremental data from station_list_silver
df_station_list_incremental = (
    spark
    .readStream
    .table(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_list_silver")
    .withColumn('merge_key', F.md5(F.concat_ws(',', 'station_id', 'dedup_hash')))
)

def upsert_to_delta(microBatchOutputDF, batchId):
  dedup_window = Window.partitionBy("station_id", "dedup_hash").orderBy("sequence_time")
  scd_window = Window.partitionBy("station_id").orderBy("sequence_time")

  microBatchOutputDF = (
      microBatchOutputDF
          .withColumn("rank", F.rank().over(dedup_window))
          .filter(F.col("rank") == 1)
          .withColumn('__START_AT', F.col('sequence_time'))
          .withColumn('__END_AT', F.lit(None).cast("timestamp"))          
          .withColumn("NEXT__START_AT", F.lead("__START_AT").over(scd_window))
  )

  microBatchOutputDF.createOrReplaceTempView("station_list")
  target_table = f"velib.`velib-{dbutils.widgets.get('environment')}`.station_list_gold"
  microBatchOutputDF.sparkSession.sql(
    f"""
      MERGE INTO {target_table} AS st
      USING station_list AS ss
      ON st.merge_key = ss.merge_key
      WHEN NOT MATCHED THEN INSERT (
        station_id,
        lastUpdatedOther,
        snapshot_timestamp,
        ttl,
        _rescued_data,
        sequence_time,
        capacity,
        lat,
        lon,
        name,
        rental_methods,
        stationCode,
        geo_point,
        merge_key,
        __START_AT,
        __END_AT,
        NEXT__START_AT
      ) VALUES
      (
        ss.station_id,
        ss.lastUpdatedOther,
        ss.snapshot_timestamp,
        ss.ttl,
        ss._rescued_data,
        ss.sequence_time,
        ss.capacity,
        ss.lat,
        ss.lon,
        ss.name,
        ss.rental_methods,
        ss.stationCode,
        ss.geo_point,
        ss.merge_key,
        ss.__START_AT,
        ss.__END_AT,
        ss.NEXT__START_AT
      )
    """
    )
  
query_station_list_incremental = (
  df_station_list_incremental
    .writeStream
    .queryName("station_list_gold")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_list_gold")
    .option("mergeSchema", "true") 
    .trigger(availableNow = True)
    .foreachBatch(upsert_to_delta)
    .start()
 )

query_station_list_incremental.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE the __END_AT date in the table
# MAGIC UPDATE velib.`velib-${environment}`.station_list_gold
# MAGIC SET `__END_AT` = NEXT__START_AT
# MAGIC WHERE `__END_AT` IS NULL AND NEXT__START_AT IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Build the append on the snapshot table
# parsing station snapshot bronze
df_live_station_gold = (
    spark.readStream.table(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_snapshot_silver")
        .select(
            'station_id',
            'snapshot_timestamp',
            'numBikesAvailable',
            'numDocksAvailable',
            'num_bikes_available',
            'num_docks_available',
            'stationCode',
            'mechanical',
            'ebike',
            'snapshot_timestamp_year',
            'snapshot_timestamp_month',
            'snapshot_timestamp_day',
            'snapshot_timestamp_dayofweek',
            'snapshot_timestamp_hour',
            'snapshot_timestamp_minute',
            'snapshot_timestamp_second',
            'is_installed',
            'is_renting',
            'is_returning',
            'last_reported',
            'lastUpdatedOther',
            'ttl'
            )
)
query_station_snapshot_gold = (
    df_live_station_gold
    .writeStream
    .queryName("station_snapshot_gold")
    .option("checkpointLocation", f"/Volumes/velib/velib-{dbutils.widgets.get('environment')}/checkpoint_directory/station_snapshot_gold")
    .option("mergeSchema", "true") 
    .trigger(availableNow = True)
    .toTable(f"velib.`velib-{dbutils.widgets.get('environment')}`.station_snapshot_gold")
)

query_station_snapshot_gold.awaitTermination()
