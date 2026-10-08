# Databricks notebook source
# Silver Transformation - Step 1
# Read Bronze Delta tables

customers_bronze = spark.read.format("delta").load(
    "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/customers"
)

products_bronze = spark.read.format("delta").load(
    "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/products"
)

orders_bronze = spark.read.format("delta").load(
    "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/orders_autoloader"
)

stores_bronze = spark.read.format("delta").load(
    "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/stores"
)

print("Bronze tables loaded successfully")

# COMMAND ----------

# Silver Transformation - Step 2
# Check Bronze schemas

customers_bronze.printSchema()
products_bronze.printSchema()
orders_bronze.printSchema()
stores_bronze.printSchema()

# COMMAND ----------

# Silver Transformation - Step 3
# Clean Customers

from pyspark.sql.functions import trim, upper

customers_silver = (
    customers_bronze
    .withColumn("customer_id", trim("customer_id"))
    .withColumn("customer_name", trim("customer_name"))
    .withColumn("city", trim("city"))
    .withColumn("state", trim("state"))
    .withColumn("customer_segment", upper(trim("customer_segment")))
    .dropDuplicates(["customer_id"])
)

display(customers_silver)

# COMMAND ----------

# Silver Transformation - Step 4
# Clean Products

from pyspark.sql.functions import trim, upper, col

products_silver = (
    products_bronze
    .withColumn("product_id", trim("product_id"))
    .withColumn("product_name", trim("product_name"))
    .withColumn("category", upper(trim("category")))
    .withColumn("subcategory", trim("subcategory"))
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))
    .dropDuplicates(["product_id"])
)

display(products_silver)

# COMMAND ----------

# Silver Transformation - Step 5
# Clean Stores

stores_silver = (
    stores_bronze
    .withColumn("store_id", trim("store_id"))
    .withColumn("store_name", trim("store_name"))
    .withColumn("city", trim("city"))
    .withColumn("state", trim("state"))
    .withColumn("region", upper(trim("region")))
    .dropDuplicates(["store_id"])
)

display(stores_silver)

# COMMAND ----------

# Silver Transformation - Step 6
# Clean Orders

from pyspark.sql.functions import trim, upper, col

orders_silver = (
    orders_bronze
    .withColumn("order_id", trim("order_id"))
    .withColumn("customer_id", trim("customer_id"))
    .withColumn("product_id", trim("product_id"))
    .withColumn("store_id", trim("store_id"))
    .withColumn("order_status", upper(trim("order_status")))
    .withColumn("payment_method", upper(trim("payment_method")))
    .withColumn("quantity", col("quantity").cast("integer"))
    .dropDuplicates(["order_id"])
)

display(orders_silver)

# COMMAND ----------

# Silver Transformation - Step 7
# Write Customers to Silver Delta

customers_silver_path = "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/customers"

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(customers_silver_path)

print("Customers Silver table written successfully")

# COMMAND ----------

# Silver Transformation - Step 8
# Write Products to Silver Delta

products_silver_path = "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/products"

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(products_silver_path)

print("Products Silver table written successfully")

# COMMAND ----------

# Silver Transformation - Step 9
# Write Orders to Silver Delta

orders_silver_path = "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/orders"

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(orders_silver_path)

print("Orders Silver table written successfully")

# COMMAND ----------

# Silver Transformation - Step 10
# Write Stores to Silver Delta

stores_silver_path = "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/stores"

stores_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(stores_silver_path)

print("Stores Silver table written successfully")

# COMMAND ----------

# Silver Transformation - Step 11
# Validate Silver Delta tables

customers_silver_check = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/customers"
)

products_silver_check = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/products"
)

orders_silver_check = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/orders"
)

stores_silver_check = spark.read.format("delta").load(
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/stores"
)

print("Customers:", customers_silver_check.count())
print("Products:", products_silver_check.count())
print("Orders:", orders_silver_check.count())
print("Stores:", stores_silver_check.count())