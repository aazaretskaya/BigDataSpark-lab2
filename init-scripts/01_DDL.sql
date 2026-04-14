-- ====================================================
-- 01_DDL.sql – создание таблиц
-- ====================================================

-- Сырая таблица для импорта данных
DROP TABLE IF EXISTS mock_data;
CREATE TABLE mock_data (
    id INTEGER,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INTEGER,
    customer_email VARCHAR(100),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(100),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20),
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price DECIMAL(10,2),
    store_name VARCHAR(200),
    store_location VARCHAR(200),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(100),
    pet_category VARCHAR(100),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating DECIMAL(3,2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(200),
    supplier_contact VARCHAR(200),
    supplier_email VARCHAR(100),
    supplier_phone VARCHAR(50),
    supplier_address TEXT,
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

-- Нормализованные таблицы (схема снежинка)
DROP TABLE IF EXISTS dim_country CASCADE;
CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dim_city CASCADE;
CREATE TABLE dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country_id INTEGER REFERENCES dim_country(country_id),
    UNIQUE (city_name, country_id)
);

DROP TABLE IF EXISTS dim_pet_type CASCADE;
CREATE TABLE dim_pet_type (
    pet_type_id SERIAL PRIMARY KEY,
    pet_type_name VARCHAR(50) NOT NULL UNIQUE
);

DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(100),
    postal_code VARCHAR(20),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    country_id INTEGER REFERENCES dim_country(country_id),
    pet_type_id INTEGER REFERENCES dim_pet_type(pet_type_id)
);

DROP TABLE IF EXISTS dim_seller CASCADE;
CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    postal_code VARCHAR(20),
    country_id INTEGER REFERENCES dim_country(country_id)
);

DROP TABLE IF EXISTS dim_supplier CASCADE;
CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    contact VARCHAR(200),
    email VARCHAR(100),
    phone VARCHAR(50),
    address TEXT,
    city_id INTEGER REFERENCES dim_city(city_id)
);

DROP TABLE IF EXISTS dim_store CASCADE;
CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    location VARCHAR(200),
    state VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(100),
    city_id INTEGER REFERENCES dim_city(city_id)
);

DROP TABLE IF EXISTS dim_category CASCADE;
CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    product_category VARCHAR(100),
    pet_category VARCHAR(100),
    UNIQUE (product_category, pet_category)
);

DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    category_id INTEGER REFERENCES dim_category(category_id),
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(3,2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

DROP TABLE IF EXISTS sales_fact CASCADE;
CREATE TABLE sales_fact (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    seller_id INTEGER REFERENCES dim_seller(seller_id),
    supplier_id INTEGER REFERENCES dim_supplier(supplier_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    quantity INTEGER,
    total_price DECIMAL(10,2)
);