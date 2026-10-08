# Databricks notebook source
# Data Quality - Step 1
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

# Data Quality - Step 2
# Check NULL values in important columns

from pyspark.sql.functions import col, sum

null_check = orders_silver.select(
    sum(col("order_id").isNull().cast("int")).alias("null_order_id"),
    sum(col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    sum(col("product_id").isNull().cast("int")).alias("null_product_id"),
    sum(col("store_id").isNull().cast("int")).alias("null_store_id"),
    sum(col("order_date").isNull().cast("int")).alias("null_order_date"),
    sum(col("quantity").isNull().cast("int")).alias("null_quantity"),
    sum(col("order_status").isNull().cast("int")).alias("null_order_status"),
    sum(col("payment_method").isNull().cast("int")).alias("null_payment_method")
)

display(null_check)

# COMMAND ----------

# Data Quality - Step 3
# Check duplicate order IDs

duplicate_orders = (
    orders_silver
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate order IDs:", duplicate_orders.count())

display(duplicate_orders)

# COMMAND ----------

# Data Quality - Step 4
# Check invalid order quantities

invalid_quantity = (
    orders_silver
    .filter(col("quantity") <= 0)
)

print("Invalid quantity records:", invalid_quantity.count())

display(invalid_quantity)

# COMMAND ----------

# Data Quality - Step 5
# Check invalid order status values

valid_statuses = ["COMPLETED", "CANCELLED", "PENDING"]

invalid_status = (
    orders_silver
    .filter(~col("order_status").isin(valid_statuses))
)

print("Invalid order status records:", invalid_status.count())

display(invalid_status)

# COMMAND ----------

# Data Quality - Step 6
# Check invalid payment method values

valid_payment_methods = ["UPI", "CREDIT CARD", "DEBIT CARD", "CASH"]

invalid_payment = (
    orders_silver
    .filter(~col("payment_method").isin(valid_payment_methods))
)

print("Invalid payment method records:", invalid_payment.count())

display(invalid_payment)

# COMMAND ----------

# Data Quality - Step 7
# Check orders with invalid customer IDs

invalid_customers = (
    orders_silver
    .join(
        customers_silver.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

print("Orders with invalid customer IDs:", invalid_customers.count())

display(invalid_customers)

# COMMAND ----------

# Data Quality - Step 8
# Check orders with invalid product IDs

invalid_products = (
    orders_silver
    .join(
        products_silver.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

print("Orders with invalid product IDs:", invalid_products.count())

display(invalid_products)

# COMMAND ----------

# Data Quality - Step 9
# Check orders with invalid store IDs

invalid_stores = (
    orders_silver
    .join(
        stores_silver.select("store_id"),
        on="store_id",
        how="left_anti"
    )
)

print("Orders with invalid store IDs:", invalid_stores.count())

display(invalid_stores)

# COMMAND ----------

# Data Quality - Step 10
# Check future order dates

from pyspark.sql.functions import current_date

future_orders = (
    orders_silver
    .filter(col("order_date") > current_date())
)

print("Future-dated orders:", future_orders.count())

display(future_orders)

# COMMAND ----------

# Data Quality - Step 11
# Overall Data Quality Summary

print("========== DATA QUALITY SUMMARY ==========")

print("Total Orders:", orders_silver.count())

print("Null Order Records:", null_check.first()["null_order_id"])
print("Duplicate Order IDs:", duplicate_orders.count())
print("Invalid Quantity Records:", invalid_quantity.count())
print("Invalid Order Status Records:", invalid_status.count())
print("Invalid Payment Method Records:", invalid_payment.count())
print("Invalid Customer IDs:", invalid_customers.count())
print("Invalid Product IDs:", invalid_products.count())
print("Invalid Store IDs:", invalid_stores.count())
print("Future-Dated Orders:", future_orders.count())

print("==========================================")

# COMMAND ----------

# Data Quality - Step 12
# Final Data Quality Status

quality_passed = (
    null_check.first()["null_order_id"] == 0
    and duplicate_orders.count() == 0
    and invalid_quantity.count() == 0
    and invalid_status.count() == 0
    and invalid_payment.count() == 0
    and invalid_customers.count() == 0
    and invalid_products.count() == 0
    and invalid_stores.count() == 0
    and future_orders.count() == 0
)

if quality_passed:
    print("DATA QUALITY STATUS: PASS")
else:
    print("DATA QUALITY STATUS: FAIL")
    print("Reason: One or more data quality rules failed.")

# COMMAND ----------

# Data Quality - Step 13
# Identify rejected orders

rejected_orders = future_orders

print("Rejected Orders:", rejected_orders.count())

display(rejected_orders)

# COMMAND ----------

# Data Quality - Step 14
# Create valid orders after quality filtering

valid_orders = (
    orders_silver
    .filter(col("order_date") <= current_date())
)

print("Valid Orders:", valid_orders.count())

display(valid_orders)

# COMMAND ----------

# Data Quality - Step 15
# Reconcile total, valid and rejected records

total_orders = orders_silver.count()
valid_count = valid_orders.count()
rejected_count = rejected_orders.count()

print("Total Orders   :", total_orders)
print("Valid Orders   :", valid_count)
print("Rejected Orders:", rejected_count)
print("Valid + Rejected:", valid_count + rejected_count)

if total_orders == valid_count + rejected_count:
    print("RECONCILIATION STATUS: PASS")
else:
    print("RECONCILIATION STATUS: FAIL")

# COMMAND ----------

# Data Quality - Step 16
# Write validated orders to Silver Delta

validated_orders_path = (
    "abfss://silver@stdatabricksretail2026.dfs.core.windows.net/validated_orders"
)

valid_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(validated_orders_path)

print("Validated orders written successfully")
print("Validated Orders:", valid_orders.count())

# COMMAND ----------

# Data Quality - Step 17
# Verify validated orders from Silver Delta

validated_orders_check = (
    spark.read
    .format("delta")
    .load(validated_orders_path)
)

print("Validated Orders in Silver:", validated_orders_check.count())

display(validated_orders_check)