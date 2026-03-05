from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def join_stop_time_trips(stop_time, trips):
    return stop_time.join(trips, on = 'trip_id', how = 'inner')


def create_stop_activity(stop_time, trips, route, stop):
    stop_activity = stop_time \
        .join(trips.select('trip_id', 'route_id'), on = 'trip_id', how = 'inner') \
        .join(stop.select('stop_id', 'stop_name'), on = 'stop_id', how = 'inner') \
        .join(route.select('route_id', 'route_type'), on = 'route_id', how = 'inner')    

    return stop_activity   


def stop_performance(stop_activity):
    stop_performance = stop_activity \
        .groupBy('route_type', 'stop_id', 'stop_name').agg(
            count('*').alias('total_stop_event'),
            countDistinct('trip_id').alias('total_trips'),
            countDistinct('route_id').alias('total_routes')
        ).orderBy(col('total_stop_event').desc())

    return stop_performance    



def stop_route_activity_per_route (stop_activity):
    stop_route_activity_per_route = stop_activity \
        .groupBy('stop_id', 'route_id', 'route_type', 'stop_name').agg(
            count('*').alias('total_stop_event'),
            countDistinct('trip_id').alias('total_trips'),
            countDistinct('route_id').alias('total_routes')
        ).orderBy(col('total_stop_event').desc())
        
    return stop_route_activity_per_route    



def mode_activity_type(stop_activity):
    mode_activity_type = stop_activity \
        .groupBy('route_type').agg(
            count('*').alias('total_stop_events'),
            countDistinct('route_id').alias('total_routes'),
            countDistinct('stop_id').alias('total_stops'),
            countDistinct('trip_id').alias('total_trips')
        ).orderBy(col('total_stop_events').desc())

    return mode_activity_type    


def total_stop(stop_activity):
    total_stop = stop_activity \
        .groupBy('stop_name').agg(
            count('*').alias('total_stop_events'),
            countDistinct('trip_id').alias('total_trips'),
            countDistinct('route_id').alias('total_routes'),
            countDistinct('stop_id').alias('total_stops')
        ).orderBy(col('total_stop_events').desc())

    return total_stop    


def num_per_stops(stop_activity):
    num_per_stops = stop_activity \
        .groupBy('stop_name').agg(
            countDistinct('stop_id').alias('total_physical_stops')
        ).orderBy(col('total_physical_stops').desc())

    return num_per_stops


def stop_time_with_hour(stop_time):
    stop_time_with_hour = stop_time \
        .withColumn('departure_ts', expr("try_to_timestamp(departure_time, 'HH:mm:ss')")) \
        .withColumn('hour_of_day', hour(col('departure_ts')))

    return stop_time_with_hour    

def hourly_activity(stop_time_with_hour):
    hourly_activity = stop_time_with_hour \
        .groupBy('hour_of_day').agg(
            count('*').alias('total_stop_events'),
            countDistinct('trip_id').alias('total_trips'),
            countDistinct('stop_id').alias('total_stops')
        )
    hourly_activity = hourly_activity.filter(col('hour_of_day').isNotNull()).orderBy(col('total_stop_events').desc())  
    return hourly_activity 