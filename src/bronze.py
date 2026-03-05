from pyspark.sql.functions import current_timestamp, lit

def ingest_to_bronze(df, table_name, source_name = 'ZTW_Warsaw_GTFS' ):
    df = df.withColumn('ingest_time', current_timestamp()) \
        .withColumn('ingest_source', lit(source_name))
    df.write.format('delta').mode('overwrite') \
        .option('overwriteSchema', 'true').saveAsTable(table_name)
    return df    

     








     
