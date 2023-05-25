from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Execute HiveQL statement to create table
create_table_query = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS taxi_green (
        VendorID BIGINT,
        lpep_pickup_datetime BIGINT,
        lpep_dropoff_datetime BIGINT,
        store_and_fwd_flag STRING,
        RatecodeID DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type DOUBLE,
        trip_type DOUBLE,
        congestion_surcharge DOUBLE
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/ahyar/green_2022'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
spark.sql(create_table_query)
# Show Tables
#spark.sql("show tables").show()

# Execute HiveQL query
#result = spark.sql("select * from taxi_green ").show()