-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # This notebook builds the Golden Layer for the velib Database
-- MAGIC 
-- MAGIC There are 2 tables that we can set for the golden layer. The station list that would represent the latest update of the historical view of the stations and the snapshot of all stations values.

-- COMMAND ----------

-- DBTITLE 1,Generating the Station List 
CREATE OR REFRESH STREAMING LIVE TABLE station_list_gold
COMMENT "Station List updated as the new API call are made based on station_id which is the unique id";

APPLY CHANGES INTO LIVE.station_list_gold
FROM STREAM(LIVE.station_list_silver)
KEYS (station_id)
IGNORE NULL UPDATES
SEQUENCE BY snapshot_timestamp
COLUMNS capacity, lat, lon, name, rental_methods, stationCode, station_id, geo_point, snapshot_timestamp, lastUpdatedOther
STORED AS SCD TYPE 1

-- COMMAND ----------

-- DBTITLE 1,Moving aggregates for Snapshot Tables
CREATE OR REFRESH STREAMING LIVE TABLE station_full_gold
COMMENT "Gold Layer flat table with all snapshot since October 2019"
AS (
SELECT 
  stationCode_station as stationCode,
  station_id,
  name,
  mechanical,
  ebike,
--   avg(mechanical) OVER (PARTITION BY (snapshot_timestamp_hour, station_id)) AS mechanical_hour,
--   avg(ebike) OVER (PARTITION BY (snapshot_timestamp_hour, station_id)) AS ebike_hour,
--   avg(num_bikes_available) OVER (PARTITION BY (snapshot_timestamp_hour, station_id)) AS num_bikes_available_hour,
--   avg(num_docks_available) OVER (PARTITION BY (snapshot_timestamp_hour, station_id)) AS num_docks_available_hour,
  num_bikes_available,
  num_docks_available,
  snapshot_timestamp,
  snapshot_timestamp_year,
  snapshot_timestamp_month,
  snapshot_timestamp_day,
  snapshot_timestamp_hour,
  snapshot_timestamp_minute,
  snapshot_timestamp_second,
  snapshot_timestamp_dayofweek,
  lastUpdatedOther_station,
  capacity,
  lat,
  lon,
  geo_point,
  h3_longlatash3(lon, lat, 9) as station_cell,
  is_installed,
  is_renting,
  is_returning,
  last_reported
FROM STREAM(LIVE.station_full_silver)
)

-- COMMAND ----------

-- CREATE OR REFRESH LIVE TABLE station_hour_gold
-- COMMENT "Gold Layer flat table with all snapshot since October 2019 organized by hour average per station and day of week"
-- AS (
-- SELECT 
--   stationCode_station as stationCode,
--   station_id,
--   name,
--   null as mechanical,
--   avg(mechanical) OVER (PARTITION BY (snapshot_timestamp_hour, snapshot_timestamp_dayofweek, station_id)) AS mechanical_hour,
--   stddev(mechanical) OVER (PARTITION BY (snapshot_timestamp_hour, snapshot_timestamp_dayofweek, station_id)) AS mechanical_hour_std,
--   snapshot_timestamp_hour,
--   snapshot_timestamp_dayofweek,
--   lat,
--   lon,
--   geo_point,
--   h3_longlatash3(lon, lat, 9) as station_cell
-- FROM LIVE.station_full_silver
-- UNION ALL
-- SELECT 
--   stationCode_station as stationCode,
--   station_id,
--   name,
--   mechanical,
--   null as mechanical_hour,
--   null as mechanical_hour_std,
--   snapshot_timestamp_hour,
--   snapshot_timestamp_dayofweek,
--   lat,
--   lon,
--   geo_point,
--   h3_longlatash3(lon, lat, 9) as station_cell
-- FROM LIVE.station_full_silver
-- WHERE to_date(snapshot_timestamp) = current_date() 
-- )
