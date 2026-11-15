# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Load Environment Variables
# secret details

# COMMAND ----------

# DBTITLE 1,Load Libraries
from pyspark.sql import *
from pyspark.sql.functions import current_timestamp, col,count

# COMMAND ----------

# DBTITLE 1,Customers section
# MAGIC %md
# MAGIC ## Customers
# MAGIC
# MAGIC Ingests `olist_customers_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.customers`.
# MAGIC
# MAGIC * **Grain:** one row per customer per order — `customer_id` is unique to each order, while `customer_unique_id` identifies the same shopper across multiple orders
# MAGIC * **Columns:** customer ID, unique customer ID, zip-code prefix, city, and state
# MAGIC * **Audit columns added:** `ingestion_date` (load timestamp) and `SourceFile` (raw file path)

# COMMAND ----------

# DBTITLE 1,Load customers bronze
cust_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_customers_dataset.csv')

cust_df = cust_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

cust_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.customers")

print(f"Customers data loaded into bronze table: {cust_df.count()}")

# COMMAND ----------

# DBTITLE 1,Geolocation section
# MAGIC %md
# MAGIC ## Geolocation
# MAGIC
# MAGIC Ingests `olist_geolocation_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.geolocation`.
# MAGIC
# MAGIC * **Grain:** one row per zip-code prefix suffix — a single prefix can map to multiple coordinate points
# MAGIC * **Columns:** zip-code prefix, lat/long coordinates, city, and state
# MAGIC * **Use:** joined downstream to enrich customers and sellers with geographic coordinates

# COMMAND ----------

# DBTITLE 1,Load GeoLocation bronze
geo_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_geolocation_dataset.csv')

geo_df = geo_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

geo_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.geolocation")

print(f"Geolocation data loaded into bronze table: {geo_df.count()}")

# COMMAND ----------

# DBTITLE 1,Product category translation section
# MAGIC %md
# MAGIC ## Product Category Name Translation
# MAGIC
# MAGIC Ingests `product_category_name_translation.csv` (71 categories) from the raw volume into the bronze table `olist_project.bronze.product_category_name_translation`.
# MAGIC
# MAGIC * **Grain:** one row per product category
# MAGIC * **Columns:** category name in Portuguese and its English translation
# MAGIC * **Use:** lookup table to translate product categories to English in the silver/gold layers

# COMMAND ----------

# DBTITLE 1,Load Product Category Translation bronze
product_category_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/product_category_name_translation.csv')

product_category_df = product_category_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

product_category_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.product_category_name_translation")

print(f"Product_Category_Translation data loaded into bronze table: {product_category_df.count()}")

# COMMAND ----------

# DBTITLE 1,Sellers section
# MAGIC %md
# MAGIC ## Sellers
# MAGIC
# MAGIC Ingests `olist_sellers_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.sellers`.
# MAGIC
# MAGIC * **Grain:** one row per seller
# MAGIC * **Columns:** seller ID, zip-code prefix, city, and state
# MAGIC * **Use:** seller dimension, joined to order items to attribute sales and freight to each seller

# COMMAND ----------

# DBTITLE 1,Load Sellers bronze
sellers_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_sellers_dataset.csv')

sellers_df = sellers_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

sellers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.sellers")

print(f"Sellers data loaded into bronze table: {sellers_df.count()}")

# COMMAND ----------

# DBTITLE 1,Products section
# MAGIC %md
# MAGIC ## Products
# MAGIC
# MAGIC Ingests `olist_products_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.products`.
# MAGIC
# MAGIC * **Grain:** one row per product
# MAGIC * **Columns:** product ID, category name (Portuguese), name length, description length, number of photos, weight in grams, and dimensions (length, height, width in cm)
# MAGIC * **Use:** core product dimension, with categories translated via the translation table

# COMMAND ----------

# DBTITLE 1,Load Products bronze
products_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_products_dataset.csv')

products_df = products_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

products_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.products")

print(f"Products data loaded into bronze table: {products_df.count()}")

# COMMAND ----------

# DBTITLE 1,Orders section
# MAGIC %md
# MAGIC ## Orders
# MAGIC
# MAGIC Ingests `olist_orders_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.orders`.
# MAGIC
# MAGIC * **Grain:** one row per order
# MAGIC * **Columns:** order ID, customer ID, order status (e.g. delivered, shipped, canceled), and the full order lifecycle timestamps — purchase, approval, carrier handoff, estimated delivery, and actual delivery
# MAGIC * **Use:** central fact table that links customers to their order events

# COMMAND ----------

# DBTITLE 1,Load Orders bronze
orders_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_orders_dataset.csv')

orders_df = orders_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

orders_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.orders")

print(f"Orders data loaded into bronze table: {orders_df.count()}")

# COMMAND ----------

# DBTITLE 1,Order items section
# MAGIC %md
# MAGIC ## Order Items
# MAGIC
# MAGIC Ingests `olist_order_items_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.orders_items`.
# MAGIC
# MAGIC * **Grain:** one row per item within an order (an order can contain several items)
# MAGIC * **Columns:** order ID, order item sequence number, product ID, seller ID, shipping limit date, price, and freight value
# MAGIC * **Use:** bridge between orders, products, and sellers — the most granular sales fact

# COMMAND ----------

orders_items_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_order_items_dataset.csv')

orders_items_df = orders_items_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

orders_items_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.orders_items")

print(f"Orders Items data loaded into bronze table: {orders_items_df.count()}")

# COMMAND ----------

# DBTITLE 1,Order reviews section
# MAGIC %md
# MAGIC ## Order Reviews
# MAGIC
# MAGIC Ingests `olist_order_reviews_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.orders_reviews`.
# MAGIC
# MAGIC * **Grain:** one row per review — a single order can have multiple reviews over time
# MAGIC * **Columns:** review ID, order ID, review score (1–5), optional review title and comment, plus creation and answer timestamps
# MAGIC * **Use:** customer-satisfaction signal for quality and churn analysis

# COMMAND ----------

orders_reviews_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_order_reviews_dataset.csv')

orders_reviews_df = orders_reviews_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

orders_reviews_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.orders_reviews")

print(f"Orders Reviews data loaded into bronze table: {orders_reviews_df.count()}")

# COMMAND ----------

# DBTITLE 1,Order payments section
# MAGIC %md
# MAGIC ## Order Payments
# MAGIC
# MAGIC Ingests `olist_order_payments_dataset.csv`  from the raw volume into the bronze table `olist_project.bronze.orders_payments`.
# MAGIC
# MAGIC * **Grain:** one row per payment within an order — an order can be split across multiple payments
# MAGIC * **Columns:** order ID, payment sequence, payment type (credit card, boleto, voucher, debit card), installments, and payment value
# MAGIC * **Use:** revenue source of truth — the sum of payment values reconciles to order totals

# COMMAND ----------

orders_payments_df = spark.read.format('csv').option('header',True).option('inferSchema',True).load('/Volumes/olist_project/raw/raw_files/raw/olist_order_payments_dataset.csv')

orders_payments_df = orders_payments_df.withColumn('ingestion_date', current_timestamp())\
            .withColumn('SourceFile', col('_metadata.file_path'))

orders_payments_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("olist_project.bronze.orders_payments")

print(f"Orders Payments data loaded into bronze table: {orders_payments_df.count()}")