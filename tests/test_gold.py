import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from src.gold import join_stop_time_trips


spark = SparkSession.builder.getOrCreate()


def test_join_stop_time_trips():
    stop_time = spark.createDataFrame(
        [(1, "S1"), (2, "S2")],
        ["trip_id", "stop_id"]
    )

    trips = spark.createDataFrame(
        [(1, "R1"), (2, "R2")],
        ["trip_id", "route_id"]
    )

    result = join_stop_time_trips(stop_time, trips)

    
    assert result.count() == 2