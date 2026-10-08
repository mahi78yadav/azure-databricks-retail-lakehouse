# Databricks notebook source
print("Azure Databricks Bronze Ingestion Started")

# COMMAND ----------

source_path = "abfss://source@stdatabricksretail2026.dfs.core.windows.net/"

print(source_path)

# COMMAND ----------

customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path + "customers/")

display(customers_df)

# COMMAND ----------

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path + "products/")

display(products_df)

# COMMAND ----------

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path + "orders/")

display(orders_df)

# COMMAND ----------

stores_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source_path + "stores/")

display(stores_df)

# COMMAND ----------

customers_bronze_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/customers"

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(customers_bronze_path)

print("Customers Bronze table written successfully")

# COMMAND ----------

products_bronze_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/products"

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(products_bronze_path)

print("Products Bronze table written successfully")

# COMMAND ----------

orders_bronze_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/orders"

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(orders_bronze_path)

print("Orders Bronze table written successfully")

# COMMAND ----------

stores_bronze_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/stores"

stores_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(stores_bronze_path)

print("Stores Bronze table written successfully")

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/customers")
)

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/products")
)

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/orders")
)

# COMMAND ----------

display(
    spark.read.format("delta")
    .load("abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/stores")
)