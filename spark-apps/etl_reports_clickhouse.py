from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, year, month, coalesce, lit, round as _round

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPERTIES = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}

spark = SparkSession.builder \
    .appName("ETL Reports to ClickHouse") \
    .master("local[2]") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.7.10.jar,/home/jovyan/jars/clickhouse-jdbc-0.9.8-all.jar") \
    .getOrCreate()

print("Reading normalized data...")
fact_df = spark.read.jdbc(url=PG_URL, table="sales_fact", properties=PG_PROPERTIES)
product_df = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPERTIES)
customer_df = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPERTIES)
country_df = spark.read.jdbc(url=PG_URL, table="dim_country", properties=PG_PROPERTIES)
store_df = spark.read.jdbc(url=PG_URL, table="dim_store", properties=PG_PROPERTIES)
city_df = spark.read.jdbc(url=PG_URL, table="dim_city", properties=PG_PROPERTIES)
supplier_df = spark.read.jdbc(url=PG_URL, table="dim_supplier", properties=PG_PROPERTIES)

fact_with_names = fact_df \
    .join(product_df, "product_id", "left") \
    .join(customer_df, "customer_id", "left") \
    .join(country_df.alias("cust_country"), customer_df.country_id == col("cust_country.country_id"), "left") \
    .join(store_df, "store_id", "left") \
    .join(city_df.alias("store_city"), store_df.city_id == col("store_city.city_id"), "left") \
    .join(country_df.alias("store_country"), col("store_city.country_id") == col("store_country.country_id"), "left") \
    .join(supplier_df, "supplier_id", "left") \
    .join(city_df.alias("supp_city"), supplier_df.city_id == col("supp_city.city_id"), "left") \
    .join(country_df.alias("supp_country"), col("supp_city.country_id") == col("supp_country.country_id"), "left")

print(f"Total fact records joined: {fact_with_names.count()}")

product_sales = fact_with_names.groupBy("product_id", product_df.name.alias("product_name")) \
    .agg(
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"),
        _sum("quantity").alias("total_quantity")
    )
product_ratings = product_df.select("product_id", "rating", "reviews")
report_product_sales = product_sales.join(product_ratings, "product_id", "left") \
    .select(
        "product_id", "product_name", "total_revenue", "total_quantity",
        coalesce(col("rating"), lit(0.0)).alias("avg_rating"),
        coalesce(col("reviews"), lit(0)).alias("reviews_count")
    ).na.fill(0).na.fill('')

customer_totals = fact_with_names.groupBy(
        "customer_id",
        customer_df.first_name.alias("first_name"),
        customer_df.last_name.alias("last_name"),
        coalesce(col("cust_country.country_name"), lit("")).alias("country")
    ).agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_spent"))
customer_orders = fact_with_names.groupBy("customer_id").agg(count("*").alias("order_count"))
customer_report = customer_totals.join(customer_orders, "customer_id") \
    .withColumn("avg_check", _round(col("total_spent") / col("order_count"), 2)) \
    .select("customer_id", "first_name", "last_name", "country", "total_spent", "avg_check") \
    .na.fill('').na.fill(0)

fact_with_names = fact_with_names.withColumn("year", year("sale_date")) \
                                 .withColumn("month", month("sale_date"))
time_sales = fact_with_names.groupBy("year", "month") \
    .agg(
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"),
        count("*").alias("total_orders"),
        avg(col("total_price").cast("decimal(20,2)")).alias("avg_order_value")
    ).na.fill(0)

store_sales = fact_with_names.groupBy(
        coalesce(col("store_id"), lit(0)).alias("store_id"),
        coalesce(store_df.name, lit("")).alias("store_name"),
        coalesce(col("store_city.city_name"), lit("")).alias("city"),
        coalesce(col("store_country.country_name"), lit("")).alias("country")
    ).agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))
store_orders = fact_with_names.groupBy("store_id").agg(count("*").alias("order_count"))
store_report = store_sales.join(store_orders, "store_id", "left") \
    .withColumn("avg_check", _round(col("total_revenue") / col("order_count"), 2)) \
    .select("store_id", "store_name", "city", "country", "total_revenue", "avg_check") \
    .na.fill('').na.fill(0)

supplier_sales = fact_with_names.groupBy(
        coalesce(col("supplier_id"), lit(0)).alias("supplier_id"),
        coalesce(supplier_df.name, lit("")).alias("supplier_name"),
        coalesce(col("supp_country.country_name"), lit("")).alias("country")
    ).agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))
supplier_avg_price = fact_with_names.groupBy("supplier_id", "product_id") \
    .agg(avg(col("price").cast("decimal(20,2)")).alias("avg_price")) \
    .groupBy("supplier_id") \
    .agg(avg("avg_price").alias("avg_product_price"))
supplier_report = supplier_sales.join(supplier_avg_price, "supplier_id", "left") \
    .select("supplier_id", "supplier_name", "country", "total_revenue",
            coalesce(col("avg_product_price"), lit(0.0)).alias("avg_product_price")) \
    .na.fill('').na.fill(0)

quality_report = product_sales.join(product_ratings, "product_id", "left") \
    .select(
        "product_id", "product_name",
        coalesce(col("rating"), lit(0.0)).alias("rating"),
        coalesce(col("reviews"), lit(0)).alias("reviews_count"),
        col("total_quantity").alias("total_quantity_sold"),
        col("total_revenue")
    ).na.fill(0).na.fill('')

def write_to_clickhouse(df, table_name):
    print(f"Writing {table_name}, rows: {df.count()}")
    df.show(5, truncate=False)
    df.write \
      .format("jdbc") \
      .option("url", CH_URL) \
      .option("dbtable", table_name) \
      .option("driver", CH_PROPERTIES["driver"]) \
      .option("user", CH_PROPERTIES["user"]) \
      .option("password", CH_PROPERTIES["password"]) \
      .mode("append") \
      .save()

write_to_clickhouse(report_product_sales, "report_product_sales")
write_to_clickhouse(customer_report, "report_customer_behavior")
write_to_clickhouse(time_sales, "report_sales_time")
write_to_clickhouse(store_report, "report_store_performance")
write_to_clickhouse(supplier_report, "report_supplier_performance")
write_to_clickhouse(quality_report, "report_product_quality")

print("All reports have been successfully written to ClickHouse.")
spark.stop()