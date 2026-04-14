from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/sparkdb"
PG_PROPERTIES = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("ETL Normalize to Snowflake") \
    .master("local[2]") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.7.10.jar,/home/jovyan/jars/clickhouse-jdbc-0.9.8-all.jar") \
    .getOrCreate()

print("Reading mock_data...")
df_raw = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPERTIES)
print(f"Raw data count: {df_raw.count()}")

countries = df_raw.select("customer_country").union(df_raw.select("seller_country")) \
                 .union(df_raw.select("store_country")).union(df_raw.select("supplier_country")) \
                 .distinct() \
                 .withColumnRenamed("customer_country", "country_name") \
                 .filter(col("country_name").isNotNull())
print(f"Countries count: {countries.count()}")
countries.write.jdbc(url=PG_URL, table="dim_country", mode="append", properties=PG_PROPERTIES)

countries_df = spark.read.jdbc(url=PG_URL, table="dim_country", properties=PG_PROPERTIES)
cities = df_raw.select("store_city", "store_country") \
    .union(df_raw.select("supplier_city", "supplier_country")) \
    .distinct() \
    .withColumnRenamed("store_city", "city_name") \
    .withColumnRenamed("store_country", "country_name") \
    .filter(col("city_name").isNotNull())

cities_with_id = cities.join(countries_df, "country_name", "left") \
                       .select("city_name", "country_id")
print(f"Cities count: {cities_with_id.count()}")
cities_with_id.write.jdbc(url=PG_URL, table="dim_city", mode="append", properties=PG_PROPERTIES)

pet_types = df_raw.select("customer_pet_type").distinct() \
                  .withColumnRenamed("customer_pet_type", "pet_type_name") \
                  .filter(col("pet_type_name").isNotNull())
print(f"Pet types count: {pet_types.count()}")
pet_types.write.jdbc(url=PG_URL, table="dim_pet_type", mode="append", properties=PG_PROPERTIES)

pet_types_df = spark.read.jdbc(url=PG_URL, table="dim_pet_type", properties=PG_PROPERTIES)

window_cust = Window.partitionBy("sale_customer_id").orderBy(col("customer_email").desc())
customers_dedup = df_raw.select(
    "sale_customer_id", "customer_first_name", "customer_last_name",
    "customer_age", "customer_email", "customer_postal_code",
    "customer_pet_name", "customer_pet_breed",
    "customer_country", "customer_pet_type"
).withColumn("rn", row_number().over(window_cust)).filter("rn = 1").drop("rn")

customers = customers_dedup \
 .join(countries_df, customers_dedup.customer_country == countries_df.country_name, "left") \
 .join(pet_types_df, customers_dedup.customer_pet_type == pet_types_df.pet_type_name, "left") \
 .select(
     col("sale_customer_id").alias("customer_id"),
     col("customer_first_name").alias("first_name"),
     col("customer_last_name").alias("last_name"),
     col("customer_age").alias("age"),
     col("customer_email").alias("email"),
     col("customer_postal_code").alias("postal_code"),
     col("customer_pet_name").alias("pet_name"),
     col("customer_pet_breed").alias("pet_breed"),
     "country_id", "pet_type_id"
 ).filter(col("customer_id").isNotNull())

print(f"Customers count: {customers.count()}")
customers.write.jdbc(url=PG_URL, table="dim_customer", mode="append", properties=PG_PROPERTIES)

window_seller = Window.partitionBy("sale_seller_id").orderBy(col("seller_email").desc())
sellers_dedup = df_raw.select(
    "sale_seller_id", "seller_first_name", "seller_last_name",
    "seller_email", "seller_postal_code", "seller_country"
).withColumn("rn", row_number().over(window_seller)).filter("rn = 1").drop("rn")

sellers = sellers_dedup \
 .join(countries_df, sellers_dedup.seller_country == countries_df.country_name, "left") \
 .select(
     col("sale_seller_id").alias("seller_id"),
     col("seller_first_name").alias("first_name"),
     col("seller_last_name").alias("last_name"),
     col("seller_email").alias("email"),
     col("seller_postal_code").alias("postal_code"),
     "country_id"
 ).filter(col("seller_id").isNotNull())
print(f"Sellers count: {sellers.count()}")
sellers.write.jdbc(url=PG_URL, table="dim_seller", mode="append", properties=PG_PROPERTIES)

cities_df = spark.read.jdbc(url=PG_URL, table="dim_city", properties=PG_PROPERTIES)
window_supp = Window.partitionBy("supplier_name", "supplier_city").orderBy(col("supplier_email").desc())
suppliers_dedup = df_raw.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city"
).withColumn("rn", row_number().over(window_supp)).filter("rn = 1").drop("rn")

suppliers = suppliers_dedup \
 .join(cities_df, suppliers_dedup.supplier_city == cities_df.city_name, "left") \
 .select(
     col("supplier_name").alias("name"),
     col("supplier_contact").alias("contact"),
     col("supplier_email").alias("email"),
     col("supplier_phone").alias("phone"),
     col("supplier_address").alias("address"),
     "city_id"
 ).filter(col("name").isNotNull())
print(f"Suppliers count: {suppliers.count()}")
suppliers.write.jdbc(url=PG_URL, table="dim_supplier", mode="append", properties=PG_PROPERTIES)

window_store = Window.partitionBy("store_name", "store_city").orderBy(col("store_email").desc())
stores_dedup = df_raw.select(
    "store_name", "store_location", "store_state",
    "store_phone", "store_email", "store_city"
).withColumn("rn", row_number().over(window_store)).filter("rn = 1").drop("rn")

stores = stores_dedup \
 .join(cities_df, stores_dedup.store_city == cities_df.city_name, "left") \
 .select(
     col("store_name").alias("name"),
     col("store_location").alias("location"),
     col("store_state").alias("state"),
     col("store_phone").alias("phone"),
     col("store_email").alias("email"),
     "city_id"
 ).filter(col("name").isNotNull())
print(f"Stores count: {stores.count()}")
stores.write.jdbc(url=PG_URL, table="dim_store", mode="append", properties=PG_PROPERTIES)

categories = df_raw.select("product_category", "pet_category").distinct() \
    .filter(col("product_category").isNotNull() | col("pet_category").isNotNull())
print(f"Categories count: {categories.count()}")
categories.write.jdbc(url=PG_URL, table="dim_category", mode="append", properties=PG_PROPERTIES)

categories_df = spark.read.jdbc(url=PG_URL, table="dim_category", properties=PG_PROPERTIES)
window_prod = Window.partitionBy("sale_product_id").orderBy(col("product_name").desc())
products_dedup = df_raw.select(
    "sale_product_id", "product_name", "product_category", "pet_category",
    "product_price", "product_weight", "product_color", "product_size",
    "product_brand", "product_material", "product_description",
    "product_rating", "product_reviews", "product_release_date", "product_expiry_date"
).withColumn("rn", row_number().over(window_prod)).filter("rn = 1").drop("rn")

products = products_dedup \
 .join(categories_df,
       (products_dedup.product_category == categories_df.product_category) &
       (products_dedup.pet_category == categories_df.pet_category),
       "left") \
 .select(
     col("sale_product_id").alias("product_id"),
     col("product_name").alias("name"),
     "category_id",
     col("product_price").alias("price"),
     col("product_weight").alias("weight"),
     col("product_color").alias("color"),
     col("product_size").alias("size"),
     col("product_brand").alias("brand"),
     col("product_material").alias("material"),
     col("product_description").alias("description"),
     col("product_rating").alias("rating"),
     col("product_reviews").alias("reviews"),
     col("product_release_date").alias("release_date"),
     col("product_expiry_date").alias("expiry_date")
 ).filter(col("product_id").isNotNull())
print(f"Products count: {products.count()}")
products.write.jdbc(url=PG_URL, table="dim_product", mode="append", properties=PG_PROPERTIES)

fact = df_raw.select(
    col("sale_date").alias("sale_date"),
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    lit(None).cast("int").alias("supplier_id"),
    lit(None).cast("int").alias("store_id"),
    col("sale_product_id").alias("product_id"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
)

print(f"Fact records count: {fact.count()}")
fact.write.jdbc(url=PG_URL, table="sales_fact", mode="append", properties=PG_PROPERTIES)

print("ETL Normalize completed successfully.")
spark.stop()