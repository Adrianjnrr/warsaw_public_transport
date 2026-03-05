# Databricks notebook source
spark.sql('create database if not exists warsaw_transport_gold')

# COMMAND ----------

from pyspark.sql.functions import *
import src.gold as gold 

stop_time = spark.table('workspace.workspace_warsaw_transport_silver.stop_times')
trips = spark.table('workspace.workspace_warsaw_transport_silver.trip')
route = spark.table('workspace.workspace_warsaw_transport_silver.routes')
stop = spark.table('workspace.workspace_warsaw_transport_silver.stops')

stop_time_trip = gold.join_stop_time_trips(stop_time, trips)
print('ROW COUNT:')
print('stop_time_trip:', stop_time_trip.count())
print('stop_time:', stop_time.count())

stop_time_trip.select('trip_id', 'route_id', 'stop_id').show(5)

# COMMAND ----------

stop_activity = gold.create_stop_activity(stop_time, trips, route, stop)
print("Rows:", stop_activity.count())
stop_activity.display()

# COMMAND ----------

stop_performance = gold.stop_performance(stop_activity)
stop_performance.show()

# COMMAND ----------

stop_route_activity_per_route = gold.stop_route_activity_per_route(stop_activity)
stop_route_activity_per_route.show()


# COMMAND ----------

mode_activity_type = gold.mode_activity_type(stop_activity)
mode_activity_type.show()

# COMMAND ----------

total_stop = gold.total_stop(stop_activity)
total_stop.show()

# COMMAND ----------

num_stop_per_stopname = gold.num_per_stops(stop_activity)

num_stop_per_stopname.show(5)

# COMMAND ----------

stop_activity_gold = stop_activity \
.withColumn('ingest_time', current_timestamp()) \
.withColumn('ingest_source', lit('ZTM_Warsaw_GTFS'))

stop_activity_gold.write.format('delta') \
    .mode('overwrite') \
        .option('overwriteSchema', 'true') \
            .saveAsTable('warsaw_transport_gold.stop_activity')

# COMMAND ----------

stop_time_with_hour = gold.stop_time_with_hour(stop_time)
stop_time_with_hour.display()

# COMMAND ----------

hourly_activity = gold.hourly_activity(stop_time_with_hour)

hourly_activity.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

pdf = hourly_activity.orderBy("hour_of_day").toPandas()
plt.plot(pdf['hour_of_day'], pdf['total_stop_events'], marker='o')
plt.xlabel("Hour of Day")
plt.ylabel("Total Stop Events")
plt.title("Network Activity by Hour")
plt.grid(True)
plt.show()

# COMMAND ----------

hourly_activity.filter(col("hour_of_day").between(6, 9)).show()
hourly_activity.filter(col("hour_of_day").between(16, 18)).show()

# COMMAND ----------

hourly_activity = hourly_activity \
.withColumn('ingest_time', current_timestamp()) \
   .withColumn('ingest_source', lit('ZTM_Warsaw_GTFS'))

hourly_activity.write.format('delta') \
    .mode('overwrite') \
        .option('overwriteSchema', 'true') \
            .saveAsTable('warsaw_transport_gold.hourly_activity') 
