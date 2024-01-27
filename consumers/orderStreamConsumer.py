from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, count, window
from pyspark.sql.functions import *
from pyspark.sql.types import *


scala_version = "2.12"
spark_version = "3.4.0"
# TODO: Ensure match above values match the correct versions
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.4.0",
]

spark = (
    SparkSession.builder.master("local")
    .appName("kafka")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)


spark = (
    SparkSession.builder.master("local")
    .appName("kafka")
    .config("spark.jars", "/Users/yashaskashyap/Downloads/postgresql-42.6.0.jar")  # Replace x.y.z with the actual version number
    .config("spark.jars.packages", ",".join(packages))
    .config('spark.local.dir', '/Users/yashaskashyap/Desktop/Extension')
    .getOrCreate()
)


# Define the schema for the Kafka messages
schema = StructType() \
    .add("orderId", StringType()) \
    .add("customerId", StringType()) \
    .add("hotelId", IntegerType()) \
    .add("hotelName", StringType()) \
    .add("items", ArrayType( \
        StructType() \
            .add("category", StringType()) \
            .add("name", StringType()) \
            .add("price", DoubleType()) \
            .add("quantity", IntegerType()))) \
    .add("netPrice", DoubleType()) \
    .add("discountPrice", DoubleType()) \
    .add("createdAt", StringType()) \
    .add("payment", StringType())
    
# Read from the Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "NEW_ORDER") \
    .load().select(from_json(col("value").cast("string"), schema).alias("json")) \
    .select("json.*")

df = df.withColumn("createdAt", to_timestamp(df["createdAt"]))

top_selling_item = df \
    .withColumn("items", explode("items")) \
    .groupBy(window("createdAt", "60 seconds"), "items.name") \
    .agg(sum("items.quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(1)

top_selling_item_query = top_selling_item \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()



##------------------------------------------------------------------------
most_selling_category = df \
    .withColumn("items", explode("items")) \
    .groupBy(window("createdAt", "60 seconds"), "items.category") \
    .agg(sum("items.quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(1)

most_selling_category_query=most_selling_category \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


##------------------------------------------------------------------------

max_order_value = df \
    .withWatermark("createdAt", "60 seconds") \
    .groupBy(window("createdAt", "60 seconds","10 seconds")) \
    .agg(max("netPrice").alias("max_net_price"))

max_order_value_query=max_order_value \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


#--------------------------------------------------------------------------
max_order_hotel = df \
    .withColumn("totalOrderValue", col("netPrice") - col("discountPrice")) \
    .groupBy("hotelName") \
    .agg(count("orderId").alias("orderCount"), sum("totalOrderValue").alias("totalOrderValue"))

max_order_value_hotel_name = max_order_hotel \
    .orderBy(desc("totalOrderValue")) \
    .limit(1) \
    .select("hotelName")
    
max_order_value_hotel_name_query = max_order_value_hotel_name \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

max_order_hotel_name = max_order_hotel \
    .orderBy(desc("orderCount")) \
    .limit(1) \
    .select("hotelName")

max_order_hotel_name_query = max_order_hotel_name \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()




##------------------------------------------------------------------------


##-------------------------------------

print("Top selling Item")
# hola = top_selling_item_query.lastProgress['timestamp']
# print("here is the time ," , hola)
top_selling_item_query.awaitTermination()


print("Most Selling Item Category")
most_selling_category_query.awaitTermination()

print("Maximum order value till now")

max_order_value_query.awaitTermination()

print("Hotel With Maximum order-value excluding discount")

max_order_value_hotel_name_query.awaitTermination()

print("Hotel WIth Maximum orders")
max_order_hotel_name_query.awaitTermination()

