# Databricks notebook source
# Auto Loader Ingestion - Step 1
# Configure source and target paths

source_path = "abfss://source@stdatabricksretail2026.dfs.core.windows.net/orders/"

bronze_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/orders_autoloader"

checkpoint_path = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/checkpoints/orders_autoloader"

print("Source:", source_path)
print("Bronze:", bronze_path)
print("Checkpoint:", checkpoint_path)

# COMMAND ----------

# Auto Loader Ingestion - Step 2
# Read new CSV files using Auto Loader

schema_location = "abfss://bronze@stdatabricksretail2026.dfs.core.windows.net/schemas/orders_autoloader"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .load(source_path)
)

print("Auto Loader stream configured successfully")

# COMMAND ----------

# Auto Loader Ingestion - Step 3
# Write streaming data to Bronze Delta

query = (
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)

query.awaitTermination()

print("Auto Loader ingestion completed successfully")

# COMMAND ----------

# Auto Loader Ingestion - Step 4
# Verify Bronze Auto Loader data

orders_autoloader_check = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

print("Auto Loader Bronze row count:", orders_autoloader_check.count())

display(orders_autoloader_check)

# COMMAND ----------

