# Лабораторная работа №2 – Apache Spark ETL

## 1. Запуск окружения

docker-compose up -d

## 2. Загрузка исходных данных в PostgreSQL

docker exec -i postgres psql -U sparkuser -d sparkdb -c "\COPY mock_data FROM '/data/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;"
for i in {1..9}; do
  docker exec -i postgres psql -U sparkuser -d sparkdb -c "\COPY mock_data FROM '/data/MOCK_DATA_$i.csv' DELIMITER ',' CSV HEADER;"
done

## 3. Нормализация данных

Зайдите в контейнер Spark:
docker exec -it spark bash
cd /home/jovyan/work

Запустите скрипт:
/usr/local/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.5,com.clickhouse:clickhouse-jdbc:0.6.5 \
  /home/jovyan/work/etl_normalize.py

## 4. Создание таблиц для витрин в ClickHouse (по 3 таблицы на каждую витрину)

Подключитесь к ClickHouse (localhost:8123, пользователь default, пароль пустой) и выполните:

CREATE TABLE IF NOT EXISTS report_product_top10_sold (
    product_id Int32,
    product_name String,
    total_quantity Int64,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS report_product_revenue_by_category (
  product_category String,
  pet_category String,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY (product_category, pet_category);

CREATE TABLE IF NOT EXISTS report_product_rating_reviews (
  product_id Int32,
  product_name String,
  avg_rating Float64,
  reviews_count Int32
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS report_customer_top10_total_spent (
    customer_id Int32,
    first_name String,
    last_name String,
  total_spent Decimal(20,2)
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS report_customer_distribution_by_country (
  country_name String,
  customers_cnt UInt64
) ENGINE = MergeTree() ORDER BY country_name;

CREATE TABLE IF NOT EXISTS report_customer_avg_check (
  customer_id Int32,
  first_name String,
  last_name String,
  avg_check Decimal(20,2)
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS report_time_monthly_yearly_trends (
    year Int32,
    month Int32,
    total_revenue Decimal(20,2),
  orders_cnt UInt64
) ENGINE = MergeTree() ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS report_time_revenue_by_year (
  year Int32,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY year;

CREATE TABLE IF NOT EXISTS report_time_avg_order_by_month (
  year Int32,
  month Int32,
  avg_order_value Float64
) ENGINE = MergeTree() ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS report_store_top5_revenue (
    store_id Int32,
    store_name String,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY store_id;

CREATE TABLE IF NOT EXISTS report_store_sales_by_city_country (
  city_name String,
  country_name String,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY (city_name, country_name);

CREATE TABLE IF NOT EXISTS report_store_avg_check (
  store_id Int32,
  store_name String,
    avg_check Decimal(20,2)
) ENGINE = MergeTree() ORDER BY store_id;

CREATE TABLE IF NOT EXISTS report_supplier_top5_revenue (
    supplier_id Int32,
    supplier_name String,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS report_supplier_avg_product_price (
  supplier_id Int32,
  avg_product_price Float64
) ENGINE = MergeTree() ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS report_supplier_sales_by_country (
  country_name String,
  total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY country_name;

CREATE TABLE IF NOT EXISTS report_quality_extreme_ratings (
  product_id Int32,
  product_name String,
  rating Float64,
  rating_group String
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS report_quality_rating_sales_corr (
  rating_sales_corr Float64
) ENGINE = MergeTree() ORDER BY tuple();

CREATE TABLE IF NOT EXISTS report_quality_top_reviews (
  product_id Int32,
  product_name String,
  reviews_count Int32
) ENGINE = MergeTree() ORDER BY product_id;

## 5. Заполнение витрин (Spark → ClickHouse)

В том же терминале контейнера spark:
/usr/local/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.5,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /home/jovyan/work/etl_reports_clickhouse.py

## 6. Проверка результатов

В ClickHouse выполните, например:

SELECT * FROM report_product_top10_sold ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM report_product_revenue_by_category ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM report_product_rating_reviews ORDER BY avg_rating DESC LIMIT 10;

SELECT * FROM report_customer_top10_total_spent ORDER BY total_spent DESC LIMIT 10;
SELECT * FROM report_customer_distribution_by_country ORDER BY customers_cnt DESC;
SELECT * FROM report_customer_avg_check ORDER BY avg_check DESC LIMIT 10;

SELECT * FROM report_time_monthly_yearly_trends ORDER BY year, month;
SELECT * FROM report_time_revenue_by_year ORDER BY year;
SELECT * FROM report_time_avg_order_by_month ORDER BY year, month;

SELECT * FROM report_store_top5_revenue ORDER BY total_revenue DESC;
SELECT * FROM report_store_sales_by_city_country ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM report_store_avg_check ORDER BY avg_check DESC LIMIT 10;

SELECT * FROM report_supplier_top5_revenue ORDER BY total_revenue DESC;
SELECT * FROM report_supplier_avg_product_price ORDER BY avg_product_price DESC LIMIT 10;
SELECT * FROM report_supplier_sales_by_country ORDER BY total_revenue DESC;

SELECT * FROM report_quality_extreme_ratings;
SELECT * FROM report_quality_rating_sales_corr;
SELECT * FROM report_quality_top_reviews ORDER BY reviews_count DESC;

## 7. Остановка контейнеров

docker-compose down
