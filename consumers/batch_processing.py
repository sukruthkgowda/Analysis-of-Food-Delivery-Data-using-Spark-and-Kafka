from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, sum, max, count, first


scala_version = "2.12"
spark_version = "3.4.0"
# TODO: Ensure match above values match the correct versions
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.4.0",
]


# Create a Spark session
# spark = SparkSession.builder \
#     .appName("Batch Computations") \
#     .getOrCreate()

spark = (
    SparkSession.builder.master("local")
    .appName("kafka")
    .config("spark.jars", "/Users/yashaskashyap/Downloads/postgresql-42.6.0.jar")  # Replace x.y.z with the actual version number
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)

# spark = (
#     SparkSession.builder.master("local")
#     .appName("kafka")
#     .config("spark.jars.packages", ",".join(packages))
#     .getOrCreate()
# )

# PostgreSQL configuration
url = "jdbc:postgresql://localhost:5333/fooddelivery"
properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Read data from the PostgreSQL database
orders_df = spark.read.jdbc(url, "Orders", properties=properties)
order_items_df = spark.read.jdbc(url, "Order_Items", properties=properties)
items_df = spark.read.jdbc(url, "Items", properties=properties)

# Join orders_df, order_items_df, and items_df
joined_df = orders_df.join(order_items_df, "orderId").join(items_df, "itemId")

# Group by day
grouped_by_day = joined_df.groupBy(date_trunc('day', col('createdAt')).alias('day'))

# Compute the number of total orders per day
total_orders_per_day = grouped_by_day.agg(count('orderId').alias('total_orders'))
# Compute the total value of orders per day
total_order_value_per_day = grouped_by_day.agg(sum(col('netPrice')).alias('total_order_value'))

# Compute the max revenue value in 1 day
max_revenue_per_day = grouped_by_day.agg(max(col('netPrice')).alias('max_revenue'))

# Compute the most sold category per day
most_sold_category_per_day = grouped_by_day.agg(first('category', True).alias('most_sold_category'))
total_orders_per_day.show()
total_order_value_per_day.show()
max_revenue_per_day.show()
most_sold_category_per_day.show()

# Stop the Spark session
spark.stop()
