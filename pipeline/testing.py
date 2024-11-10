# Databricks notebook source
df = (
    spark.readStream
        .format('cloudFiles')
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", True)
        .option("cloudFiles.schemaLocation", "/Volumes/velib/velib-dev/velib_landing_zone/schema/")
        .option("cloudFiles.schemaHints", "lastUpdatedOther TIMESTAMP, snapshot_timestamp TIMESTAMP, ttl INT")
        .option("pathGlobFilter", "list_station*")
        .load("/Volumes/velib/velib-dev/sandbox/")
)

# COMMAND ----------

import re
from datetime import datetime
import pyspark.sql.functions as F

display(df.select(
    '*', 
    F.expr("""
        regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0)
    """).alias('snapshot_extract'),
    F.expr("""
        to_timestamp(regexp_extract(_metadata.file_name, '\\\d{4}-\\\d{2}-\\\d{2}_\\\d{2}h\\\d{2}m\\\d{2}s', 0),
        'yyyy-MM-dd_HH\\'h\\'mm\\'m\\'ss\\'s\\'')
    """).alias('snapshot'),
    '_metadata'
))

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cp /Volumes/velib/velib-prod/velib_landing_zone/station_list/2024-04-26/station_status_2021-06-11_15h35m37s.json /Volumes/velib/velib-dev-basic/landing_zone/station_list/

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /Volumes/velib/velib-dev-basic/landing_zone/station_list/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM velib.`velib-dev-basic`.station_snapshot_gold
# MAGIC WHERE station_id = 256403519

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM velib.`velib-dev-basic`.station_list_gold
# MAGIC WHERE station_id = 54000556

# COMMAND ----------

dbutils.fs.cp('/Volumes/velib/velib-dev/landing_zone/', '/Volumes/velib/velib-prod/velib_landing_zone/', recurse=True) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC * 
# MAGIC FROM read_state_metadata('/Volumes/velib/velib-prod/checkpoint_directory/stat')

# COMMAND ----------

display(spark.read
  .format("state-metadata")
  .load("/Volumes/velib/velib-prod/checkpoint_directory/station_list_gold")
)

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /Volumes/velib/velib-prod/checkpoint_directory/station_snapshot_silver/state/0/0/_metadata

# COMMAND ----------


