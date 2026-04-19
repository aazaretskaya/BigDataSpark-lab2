from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    corr,
    count,
    countDistinct,
    max as _max,
    month,
    round as _round,
    sum as _sum,
    year,
    when,
)

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
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.7.10.jar,/home/jovyan/jars/clickhouse-jdbc-0.9.8-all.jar") \
    .getOrCreate()

print("Reading normalized data...")
fact_df = spark.read.jdbc(url=PG_URL, table="sales_fact", properties=PG_PROPERTIES)
product_df = spark.read.jdbc(url=PG_URL, table="dim_product", properties=PG_PROPERTIES)
category_df = spark.read.jdbc(url=PG_URL, table="dim_category", properties=PG_PROPERTIES)
customer_df = spark.read.jdbc(url=PG_URL, table="dim_customer", properties=PG_PROPERTIES)
country_df = spark.read.jdbc(url=PG_URL, table="dim_country", properties=PG_PROPERTIES)
store_df = spark.read.jdbc(url=PG_URL, table="dim_store", properties=PG_PROPERTIES)
city_df = spark.read.jdbc(url=PG_URL, table="dim_city", properties=PG_PROPERTIES)
supplier_df = spark.read.jdbc(url=PG_URL, table="dim_supplier", properties=PG_PROPERTIES)

sales_product = fact_df \
    .join(product_df.select("product_id", "name", "category_id", "rating", "reviews"), "product_id", "left") \
    .join(category_df.select("category_id", "product_category", "pet_category"), "category_id", "left") \
    .withColumnRenamed("name", "product_name")

sales_customer = fact_df \
    .join(customer_df.select("customer_id", "first_name", "last_name", "country_id"), "customer_id", "left") \
    .join(country_df.select("country_id", "country_name"), "country_id", "left")

sales_store = fact_df \
    .join(store_df.select("store_id", "name", "city_id"), "store_id", "left") \
    .join(city_df.select("city_id", "city_name", "country_id"), "city_id", "left") \
    .join(country_df.select("country_id", "country_name"), "country_id", "left") \
    .withColumnRenamed("name", "store_name")

sales_supplier = fact_df \
    .join(supplier_df.select("supplier_id", "name", "city_id"), "supplier_id", "left") \
    .join(city_df.select("city_id", "city_name", "country_id"), "city_id", "left") \
    .join(country_df.select("country_id", "country_name"), "country_id", "left") \
    .withColumnRenamed("name", "supplier_name")

# 1) Product sales showcase (3 tables)
report_product_top10_sold = sales_product.groupBy("product_id", "product_name") \
    .agg(
        _sum("quantity").alias("total_quantity"),
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue")
    ) \
    .orderBy(col("total_quantity").desc()) \
    .limit(10)

report_product_revenue_by_category = sales_product.groupBy("product_category", "pet_category") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))

report_product_rating_reviews = sales_product.groupBy("product_id", "product_name") \
    .agg(
        avg(col("rating").cast("double")).alias("avg_rating"),
        _max(col("reviews").cast("int")).alias("reviews_count")
    )

# 2) Customer sales showcase (3 tables)
report_customer_top10_total_spent = sales_customer.groupBy(
        "customer_id",
        "first_name",
        "last_name"
    ) \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_spent")) \
    .orderBy(col("total_spent").desc()) \
    .limit(10)

report_customer_distribution_by_country = sales_customer.groupBy("country_name") \
    .agg(countDistinct("customer_id").alias("customers_cnt"))

report_customer_avg_check = sales_customer.groupBy(
        "customer_id",
        "first_name",
        "last_name"
    ).agg(
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_spent"),
        count("*").alias("orders_cnt")
    ).withColumn("avg_check", _round(col("total_spent") / col("orders_cnt"), 2)) \
     .select("customer_id", "first_name", "last_name", "avg_check")

# 3) Time sales showcase (3 tables)
sales_time = fact_df.withColumn("year", year("sale_date")).withColumn("month", month("sale_date"))

report_time_monthly_yearly_trends = sales_time.groupBy("year", "month") \
    .agg(
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"),
        count("*").alias("orders_cnt")
    )

report_time_revenue_by_year = sales_time.groupBy("year") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))

report_time_avg_order_by_month = sales_time.groupBy("year", "month") \
    .agg(avg(col("total_price").cast("decimal(20,2)")).alias("avg_order_value"))

# 4) Store sales showcase (3 tables)
report_store_top5_revenue = sales_store.groupBy("store_id", "store_name") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

report_store_sales_by_city_country = sales_store.groupBy("city_name", "country_name") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))

report_store_avg_check = sales_store.groupBy("store_id", "store_name") \
    .agg(
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"),
        count("*").alias("orders_cnt")
    ).withColumn("avg_check", _round(col("total_revenue") / col("orders_cnt"), 2)) \
     .select("store_id", "store_name", "avg_check")

# 5) Supplier sales showcase (3 tables)
report_supplier_top5_revenue = sales_supplier.groupBy("supplier_id", "supplier_name") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

report_supplier_avg_product_price = fact_df \
    .join(product_df.select("product_id", "price"), "product_id", "left") \
    .groupBy("supplier_id") \
    .agg(avg(col("price").cast("decimal(20,2)")).alias("avg_product_price"))

report_supplier_sales_by_country = sales_supplier.groupBy("country_name") \
    .agg(_sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue"))

# 6) Product quality showcase (3 tables)
quality_base = fact_df \
    .join(product_df.select("product_id", "name", "rating", "reviews"), "product_id", "left") \
    .withColumnRenamed("name", "product_name") \
    .groupBy("product_id", "product_name", "rating", "reviews") \
    .agg(
        _sum("quantity").alias("total_quantity_sold"),
        _sum(col("total_price").cast("decimal(20,2)")).alias("total_revenue")
    )

max_rating = quality_base.agg({"rating": "max"}).collect()[0][0]
min_rating = quality_base.agg({"rating": "min"}).collect()[0][0]

report_quality_extreme_ratings = quality_base \
    .withColumn(
        "rating_group",
        when(col("rating") == max_rating, "max_rating")
        .when(col("rating") == min_rating, "min_rating")
    ) \
    .filter(col("rating_group").isNotNull()) \
    .select("product_id", "product_name", "rating", "rating_group")

report_quality_rating_sales_corr = quality_base \
    .select(corr(col("rating").cast("double"), col("total_quantity_sold").cast("double")).alias("rating_sales_corr"))

report_quality_top_reviews = quality_base \
    .select(
        "product_id",
        "product_name",
        col("reviews").cast("int").alias("reviews_count")
    ) \
    .orderBy(col("reviews_count").desc()) \
    .limit(10)

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

write_to_clickhouse(report_product_top10_sold, "report_product_top10_sold")
write_to_clickhouse(report_product_revenue_by_category, "report_product_revenue_by_category")
write_to_clickhouse(report_product_rating_reviews, "report_product_rating_reviews")

write_to_clickhouse(report_customer_top10_total_spent, "report_customer_top10_total_spent")
write_to_clickhouse(report_customer_distribution_by_country, "report_customer_distribution_by_country")
write_to_clickhouse(report_customer_avg_check, "report_customer_avg_check")

write_to_clickhouse(report_time_monthly_yearly_trends, "report_time_monthly_yearly_trends")
write_to_clickhouse(report_time_revenue_by_year, "report_time_revenue_by_year")
write_to_clickhouse(report_time_avg_order_by_month, "report_time_avg_order_by_month")

write_to_clickhouse(report_store_top5_revenue, "report_store_top5_revenue")
write_to_clickhouse(report_store_sales_by_city_country, "report_store_sales_by_city_country")
write_to_clickhouse(report_store_avg_check, "report_store_avg_check")

write_to_clickhouse(report_supplier_top5_revenue, "report_supplier_top5_revenue")
write_to_clickhouse(report_supplier_avg_product_price, "report_supplier_avg_product_price")
write_to_clickhouse(report_supplier_sales_by_country, "report_supplier_sales_by_country")

write_to_clickhouse(report_quality_extreme_ratings, "report_quality_extreme_ratings")
write_to_clickhouse(report_quality_rating_sales_corr, "report_quality_rating_sales_corr")
write_to_clickhouse(report_quality_top_reviews, "report_quality_top_reviews")

print("All reports have been successfully written to ClickHouse.")
spark.stop()