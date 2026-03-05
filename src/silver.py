from pyspark.sql.functions import split, col

def select_stop_time(stop_time):
    return (
        stop_time
        .withColumn('hour', split(col('arrival_time'),":")[0].cast('int'))
        .groupBy('hour')
        .count()
        .orderBy('hour')
    )
    