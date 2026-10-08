# Databricks notebook source
# Gold Business Layer - Step 1
# Load Silver Delta tables

customers_silver = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/customers"
)

products_silver = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/products"
)

orders_silver = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/orders"
)

stores_silver = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/stores"
)

print("Silver tables loaded successfully")

# COMMAND ----------

# Gold Business Layer - Step 2
# Inspect Silver schemas before joining

customers_silver.printSchema()
products_silver.printSchema()
orders_silver.printSchema()
stores_silver.printSchema()

# COMMAND ----------

# Gold Business Layer - Step 3
# Create Gold Sales dataset by joining Silver tables

from pyspark.sql.functions import col

gold_sales = (
    orders_silver.alias("o")
    .join(
        customers_silver.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        products_silver.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left"
    )
    .join(
        stores_silver.alias("s"),
        col("o.store_id") == col("s.store_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.customer_segment"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.subcategory"),
        col("p.unit_price"),
        col("o.quantity"),
        col("o.order_status"),
        col("o.payment_method"),
        col("o.store_id"),
        col("s.store_name"),
        col("s.city").alias("store_city"),
        col("s.state").alias("store_state"),
        col("s.region"),
        (col("o.quantity") * col("p.unit_price")).alias("sales_amount")
    )
)

display(gold_sales)

# COMMAND ----------

# Gold Business Layer - Step 4
# Validate Gold Sales dataset

print("Gold row count:", gold_sales.count())

print("Gold schema:")
gold_sales.printSchema()

print("Null customer IDs:", gold_sales.filter(col("customer_id").isNull()).count())
print("Null product IDs:", gold_sales.filter(col("product_id").isNull()).count())
print("Null store IDs:", gold_sales.filter(col("store_id").isNull()).count())

# COMMAND ----------

# Gold Business Layer - Step 5
# Check join quality

print("Null customer IDs:", gold_sales.filter(col("customer_id").isNull()).count())
print("Null product IDs:", gold_sales.filter(col("product_id").isNull()).count())
print("Null store IDs:", gold_sales.filter(col("store_id").isNull()).count())
print("Null customer names:", gold_sales.filter(col("customer_name").isNull()).count())
print("Null product names:", gold_sales.filter(col("product_name").isNull()).count())
print("Null store names:", gold_sales.filter(col("store_name").isNull()).count())

# COMMAND ----------

# Gold Business Layer - Step 6
# Write Gold Sales dataset to Delta

gold_sales_path = "abfss://gold@stdatabricksretail2026.dfs.core.windows.net/sales"

gold_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_sales_path)

print("Gold Sales table written successfully")

# COMMAND ----------

# Gold Business Layer - Step 7
# Validate Gold Delta table

gold_sales_check = spark.read.format("delta").load(
    "abfss://gold@stdatabricksretail2026.dfs.core.windows.net/sales"
)

print("Gold Sales row count:", gold_sales_check.count())

display(gold_sales_check)

# COMMAND ----------

# Gold Business Layer - Step 8
# Calculate business metrics

from pyspark.sql.functions import sum, count, avg, round

gold_metrics = gold_sales_check.agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_quantity"),
    round(sum(col("quantity") * col("unit_price")), 2).alias("total_sales"),
    round(avg(col("unit_price")), 2).alias("average_unit_price")
)

display(gold_metrics)

# COMMAND ----------

# Gold Business Layer - Step 9
# Sales by Product Category

category_sales = (
    gold_sales_check
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("category")
    .agg(
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(category_sales)

# COMMAND ----------

# Gold Business Layer - Step 10
# Sales by Store

store_sales = (
    gold_sales_check
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("store_id", "store_name", "region")
    .agg(
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(store_sales)

# COMMAND ----------

# Gold Business Layer - Step 11
# Sales by Region

region_sales = (
    gold_sales_check
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("region")
    .agg(
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(region_sales)

# COMMAND ----------

# Gold Business Layer - Step 12
# Verify total sales calculation

gold_sales_check.select(
    "order_id",
    "quantity",
    "unit_price"
).withColumn(
    "sales_amount",
    col("quantity") * col("unit_price")
).agg(
    sum("sales_amount").alias("total_sales")
).show()

# COMMAND ----------

# Gold Business Layer - Step 13
# Sales by Customer

customer_sales = (
    gold_sales_check
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("customer_id", "customer_name", "customer_segment")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(customer_sales)

# COMMAND ----------

# Gold Business Layer - Step 14
# Register Gold Sales as a Unity Catalog table

spark.sql("""
CREATE TABLE IF NOT EXISTS dbw_retail_lakehouse_dev.default.gold_sales
USING DELTA
LOCATION 'abfss://gold@stdatabricksretail2026.dfs.core.windows.net/sales'
""")

print("Gold Sales Unity Catalog table registered successfully")

# COMMAND ----------

# Gold Business Layer - Step 15
# Verify Unity Catalog Gold table

gold_uc = spark.table("dbw_retail_lakehouse_dev.default.gold_sales")

print("Unity Catalog Gold row count:", gold_uc.count())

display(gold_uc)

# COMMAND ----------

# Gold Business Layer - Step 16
# Top 5 Customers by Sales

top_customers = (
    gold_uc
    .groupBy("customer_id", "customer_name", "customer_segment")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum(col("quantity") * col("unit_price")), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
    .limit(5)
)

display(top_customers)

# COMMAND ----------

# Gold Business Layer - Step 17
# Top 5 Products by Sales

top_products = (
    gold_uc
    .groupBy("product_id", "product_name", "category")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum(col("quantity") * col("unit_price")), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
    .limit(5)
)

display(top_products)

# COMMAND ----------

# Gold Business Layer - Step 18
# Sales by Payment Method

payment_sales = (
    gold_uc
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(payment_sales)

# COMMAND ----------

# Gold Business Layer - Step 19
# Sales by Order Status

status_sales = (
    gold_uc
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .groupBy("order_status")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(status_sales)

# COMMAND ----------

# Gold Business Layer - Step 20
# Create consolidated business metrics

business_metrics = gold_uc.agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_quantity"),
    round(sum(col("quantity") * col("unit_price")), 2).alias("total_sales"),
    round(avg("unit_price"), 2).alias("average_unit_price")
)

display(business_metrics)

# COMMAND ----------

# Gold Business Layer - Step 21
# Write consolidated business metrics to Delta

business_metrics_path = "abfss://gold@stdatabricksretail2026.dfs.core.windows.net/business_metrics"

business_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .save(business_metrics_path)

print("Business metrics written successfully")

# COMMAND ----------

# Gold Business Layer - Step 22
# Verify saved business metrics

business_metrics_check = spark.read.format("delta").load(
    "abfss://gold@stdatabricksretail2026.dfs.core.windows.net/business_metrics"
)

print("Business Metrics row count:", business_metrics_check.count())

display(business_metrics_check)

# COMMAND ----------

# Gold Business Layer - Step 23
# Register Business Metrics as a Unity Catalog table

spark.sql("""
CREATE TABLE IF NOT EXISTS dbw_retail_lakehouse_dev.default.business_metrics
USING DELTA
LOCATION 'abfss://gold@stdatabricksretail2026.dfs.core.windows.net/business_metrics'
""")

print("Business Metrics Unity Catalog table registered successfully")

# COMMAND ----------

# Gold Business Layer - Step 24
# Verify Business Metrics Unity Catalog table

business_metrics_uc = spark.table(
    "dbw_retail_lakehouse_dev.default.business_metrics"
)

print("Unity Catalog Business Metrics row count:", business_metrics_uc.count())

display(business_metrics_uc)

# COMMAND ----------

# Gold Business Layer - Step 25
# List Gold tables registered in Unity Catalog

spark.sql("""
SHOW TABLES IN dbw_retail_lakehouse_dev.default
""").show(truncate=False)