import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, from_unixtime

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()


df = spark.sql("select * from default.taxi_green")

#count how much null value in ehail_fee
#null_count = df.select(sum(col('ehail_fee').isNull().cast("integer")).alias('ehail_fee_null_count')).collect()[0][0]

# Print the null count
#print("Null count in 'ehail_fee' column:", null_count)

# drop column ehail_fee
df = df.drop('ehail_fee')

#rename columns
df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
       .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

# Convert columns to datetime
df = df.withColumn("pickup_datetime", from_unixtime(df.pickup_datetime / 1000000).cast("timestamp")) \
       .withColumn("dropoff_datetime", from_unixtime(df.dropoff_datetime / 1000000).cast("timestamp"))

# Show the updated DataFrame


# 1.Making dim_vendor

df = df.withColumn("vendor_name", when(col("VendorID") == 1, "Creative Mobile Technologies, LLC") \
                                 .when(col("VendorID") == 2, "VeriFone Inc") \
                                 .otherwise("Unknown"))

df = df.withColumn("sk_vendor", when(col("VendorID") == 1, 1) \
                                .when(col("VendorID") == 2, 2) \
                                .otherwise(3))

dim_vendor = df.selectExpr("sk_vendor","VendorID", "vendor_name").distinct().orderBy("sk_vendor")
dim_vendor.show()

# 2.Making dim_store_and_forward

df = df.withColumn("store_and_forward", when(col("store_and_fwd_flag") == "Y", "store and forward trip") \
                                 .when(col("store_and_fwd_flag") == "N", "not a store and forward trip") \
                                 .otherwise("Unknown"))

df = df.withColumn("sk_store_and_forward", when(col("store_and_fwd_flag") == "Y", 1) \
                                .when(col("store_and_fwd_flag") == "N" , 2) \
                                .otherwise(3))

dim_store_and_forward = df.selectExpr("sk_store_and_forward", "store_and_forward", "store_and_fwd_flag").distinct().orderBy("sk_store_and_forward")
dim_store_and_forward = dim_store_and_forward.filter(col("store_and_fwd_flag").isNotNull())
dim_store_and_forward.show()

# 3.Making dim_trip_type

df = df.withColumn("trip_name", when(col("trip_type") == 1, "Street-hail") \
                                 .when(col("trip_type") == 2, "Dispatch") \
                                 .otherwise("Unknown"))

df = df.withColumn("sk_trip_type", when(col("trip_type") == 1, 1) \
                                .when(col("trip_type") == 2, 2) \
                                .otherwise(3))

dim_trip_type = df.selectExpr("sk_trip_type","trip_type", "trip_name").distinct().orderBy("sk_trip_type")
dim_trip_type = dim_trip_type.filter(col("trip_type").isNotNull())
dim_trip_type.show()

# 4.Making dim_rate_code
df = df.filter(col("RateCodeID") != 99.0)
df = df.withColumn("rate_code_name", when(col("RatecodeID") == 1, "Standard ratel") \
                                 .when(col("RatecodeID") == 2, "JFK") \
                                 .when(col("RatecodeID") == 3, "Newark") \
                                 .when(col("RatecodeID") == 4, "Nassau or Westchester") \
                                 .when(col("RatecodeID") == 5, "Negotiated fare") \
                                 .when(col("RatecodeID") == 6, "Group ride") \
                                 .otherwise("Unknown"))

df = df.withColumn("sk_rate_code", when(col("RatecodeID") == 1, 1) \
                                .when(col("RatecodeID") == 2, 2) \
                                .when(col("RatecodeID") == 3, 3) \
                                .when(col("RatecodeID") == 4, 4) \
                                .when(col("RatecodeID") == 5, 5) \
                                .when(col("RatecodeID") == 6, 6) \
                                .otherwise(7))

dim_rate_code_id = df.selectExpr("sk_rate_code","RatecodeID", "rate_code_name").distinct().orderBy("sk_rate_code")
dim_rate_code_id.show()

# 5.Making dim_payment_type

df = df.withColumn("payment_type_name", when(col("payment_type") == 1, "Credit card") \
                                 .when(col("payment_type") == 2, "Cash") \
                                 .when(col("payment_type") == 3, "No charge") \
                                 .when(col("payment_type") == 4, "Dispute") \
                                 .when(col("payment_type") == 5, "Unknown") \
                                 .when(col("payment_type") == 6, "Voided trip") \
                                 .otherwise("Other"))

df = df.withColumn("sk_payment_type", when(col("payment_type") == 1, 1) \
                                .when(col("payment_type") == 2, 2) \
                                .when(col("payment_type") == 3, 3) \
                                .when(col("payment_type") == 4, 4) \
                                .when(col("payment_type") == 5, 5) \
                                .when(col("payment_type") == 6, 6) \
                                .otherwise(7))

dim_payment_type = df.selectExpr("sk_payment_type","payment_type", "payment_type_name").distinct().orderBy("sk_payment_type")


# Add new row
new_row_df = spark.createDataFrame([(6.0, 6.0, "Voided trip")], ["sk_payment_type", "payment_type", "payment_type_name"])

# Union the new DataFrame with dim_rate_code_id
dim_payment_type = dim_payment_type.union(new_row_df)
dim_payment_type = dim_payment_type.orderBy(col("sk_payment_type"))
dim_payment_type.show()

# 6.making fact_table
fact_table = df.selectExpr("sk_vendor", "sk_store_and_forward", "sk_trip_type", "sk_rate_code", "sk_payment_type", "pickup_datetime", "dropoff_datetime",
                         "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "extra",
                         "mta_tax", "improvement_surcharge", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge")

fact_table.show()




# Create New Database In Hive
spark.sql("create database data_warehouse")
spark.sql("show databases").show()
spark.sql("use data_warehouse")

# save in hive
# dim_vendor.write.mode("overwrite").saveAsTable("dim_vendor")
# spark.sql("select * from dim_vendor").show()

dim_store_and_forward.write.mode("overwrite").saveAsTable("dim_store_and_forward")
dim_trip_type.write.mode("overwrite").saveAsTable("dim_trip_type")
dim_rate_code_id.write.mode("overwrite").saveAsTable("dim_rate_code_id")
dim_payment_type.write.mode("overwrite").saveAsTable("dim_payment_type")
fact_table.write.mode("overwrite").saveAsTable("fact_table")

