# Databricks notebook source
spark.sql('create database if not exists warsaw_transport_gold')

# COMMAND ----------

from pyspark.sql.functions import *

stop_time = spark.table('workspace.workspace_warsaw_transport_silver.stop_times')
trips = spark.table('workspace.workspace_warsaw_transport_silver.trip')
route = spark.table('workspace.workspace_warsaw_transport_silver.routes')
stop = spark.table('workspace.workspace_warsaw_transport_silver.stops')

stop_time_trip = stop_time.join(trips, on="trip_id", how='inner')
print('ROW COUNT:')
print('stop_time_trip:', stop_time_trip.count())
print('stop_time:', stop_time.count())

stop_time_trip.select('trip_id', 'route_id', 'stop_id').show(5)

# COMMAND ----------

stop_activity = stop_time \
    .join(trips.select("trip_id", "route_id"), on="trip_id", how="inner") \
    .join(route.select("route_id", "route_type"), on="route_id", how="inner") \
    .join(stop.select("stop_id", "stop_name"), on="stop_id", how="inner")

print("Rows:", stop_activity.count())
stop_activity.display()

# COMMAND ----------

stop_performance = stop_activity.groupBy(
    'stop_id',
    'stop_name',
    'route_type'
).agg(
    count('*').alias('total_stop_events'),
    countDistinct('trip_id').alias('total_trips'),
    countDistinct('route_id').alias('total_routes')
).orderBy(col('total_stop_events').desc())
stop_performance.show()

# COMMAND ----------

stop_route_activity_per_route = stop_activity.groupBy('stop_id', 'route_id', 'route_type', 'stop_name').agg(
    count('*').alias('total_stop_events'),
    countDistinct('trip_id').alias('total_trips'),
)
stop_route_activity_per_route.orderBy(col('total_stop_events').desc()).show()


# COMMAND ----------

overal_stop_activity = stop_activity.groupBy('stop_name', 'stop_id','route_type').agg(
    count('*').alias('total_stop_events'),
    countDistinct('trip_id').alias('total_trips'),
    countDistinct('route_id').alias('total_routes')
)
overal_stop_activity.orderBy(col('total_stop_events').desc()).show()

# COMMAND ----------

mode_activity_type = stop_activity.groupBy('route_type').agg(
    count('*').alias('total_stop_events'),
    countDistinct('route_id').alias('total_routes'),
    countDistinct('stop_id').alias('total_stops')
)
mode_activity_type.orderBy(col('total_stop_events').desc()).show()

# COMMAND ----------

total_stop = stop_activity.groupBy('stop_name').agg(
    count('*').alias('total_stop_events'),
    countDistinct('stop_id').alias('total_stops'),
    countDistinct('route_id').alias('total_routes')
)
total_stop.orderBy(col('total_stop_events').desc()).show()

# COMMAND ----------

num_stop_per_stopname = stop_activity.groupBy('stop_name').agg(
    countDistinct('stop_id').alias('total_physical_stop')
)

num_stop_per_stopname.orderBy(col('total_physical_stop').desc()).show(5)

# COMMAND ----------

stop_activity_gold = stop_activity \
.withColumn('ingest_time', current_timestamp()) \
.withColumn('ingest_source', lit('ZTM_Warsaw_GTFS'))

stop_activity_gold.write.format('delta') \
    .mode('overwrite') \
        .option('overwriteSchema', 'true') \
            .saveAsTable('warsaw_transport_gold.stop_activity')

# COMMAND ----------

stop_time_with_hour = stop_time.withColumn('departure_ts', expr("try_to_timestamp(departure_time, 'HH:mm:ss')"))
stop_time_with_hour = stop_time_with_hour.withColumn('hour_of_day', hour(col('departure_ts')))
stop_time_with_hour.show()

# COMMAND ----------

hourly_activity = stop_time_with_hour.groupBy('hour_of_day').agg(
    count('*').alias('total_stop_events'),
    countDistinct('trip_id').alias('total_trips')
)

hourly_activity.filter(col('hour_of_day').isNotNull()).orderBy(col('total_stop_events').desc()).show()

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
