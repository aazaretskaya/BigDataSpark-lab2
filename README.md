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

## 4. Создание таблиц для витрин в ClickHouse

Подключитесь к ClickHouse (localhost:8123, пользователь default, пароль пустой) и выполните:

CREATE TABLE IF NOT EXISTS report_product_sales (
    product_id Int32,
    product_name String,
    total_revenue Decimal(20,2),
    total_quantity Int64,
    avg_rating Float32,
    reviews_count Int32
) ENGINE = MergeTree() ORDER BY product_id;

CREATE TABLE IF NOT EXISTS report_customer_behavior (
    customer_id Int32,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal(20,2),
    avg_check Decimal(20,2)
) ENGINE = MergeTree() ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS report_sales_time (
    year Int32,
    month Int32,
    total_revenue Decimal(20,2),
    total_orders Int64,
    avg_order_value Decimal(20,2)
) ENGINE = MergeTree() ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS report_store_performance (
    store_id Int32,
    store_name String,
    city String,
    country String,
    total_revenue Decimal(20,2),
    avg_check Decimal(20,2)
) ENGINE = MergeTree() ORDER BY store_id;

CREATE TABLE IF NOT EXISTS report_supplier_performance (
    supplier_id Int32,
    supplier_name String,
    country String,
    total_revenue Decimal(20,2),
    avg_product_price Decimal(20,2)
) ENGINE = MergeTree() ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS report_product_quality (
    product_id Int32,
    product_name String,
    rating Float32,
    reviews_count Int32,
    total_quantity_sold Int64,
    total_revenue Decimal(20,2)
) ENGINE = MergeTree() ORDER BY product_id;

## 5. Заполнение витрин (Spark → ClickHouse)

В том же терминале контейнера spark:
/usr/local/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.5,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  /home/jovyan/work/etl_reports_clickhouse.py

## 6. Проверка результатов

В ClickHouse выполните, например:

SELECT * FROM report_product_sales ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM report_customer_behavior ORDER BY total_spent DESC LIMIT 10;
SELECT * FROM report_sales_time;

## 7. Остановка контейнеров

docker-compose down
