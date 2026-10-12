# Azure Databricks Retail Lakehouse

## Beginner-friendly implementation handbook: Source → Bronze → Silver → Gold

> **Purpose:** Build the project from the beginning using the four
> supplied CSV files, Unity Catalog, ADLS Gen2, Delta Lake, and Auto
> Loader. This guide explains what each object does, gives the SQL
> commands used to inspect it, and provides a single implementation
> path.
>
> **Environment used in this guide** - Storage account:
> `stdatabricksretailsit` - Catalog: `retailsit` - External locations:
> `ext_retailsit_bronze`, `ext_retailsit_silver`, `ext_retailsit_gold` -
> Containers/paths: `source`, `bronze`, `silver`, `gold` - Tables:
> customers, products, orders, stores
>
> Check these names in your own workspace before running commands. The
> external location names and ADLS paths are separate things.

------------------------------------------------------------------------

## 1. The whole project in one picture

``` text
ADLS Gen2 source container (CSV files)
   |
   | Auto Loader discovers new files and tracks progress in a checkpoint
   v
Bronze Delta tables (raw, append-oriented ingestion)
   |
   | PySpark cleaning, validation, deduplication and type conversion
   v
Silver Delta tables (clean, consistent records)
   |
   | joins, calculations and aggregations
   v
Gold Delta tables (business-ready reporting)
```

### Explain it to a high-school student

-   **Source CSV:** like the original Excel sheets delivered to a shop.
-   **External location:** a Unity Catalog security object that says
    which cloud-storage path Databricks may access and which storage
    credential it uses.
-   **Catalog:** like a top-level library.
-   **Schema:** like a shelf or folder inside the library.
-   **Table:** like a named dataset you can query with SQL.
-   **Delta table:** data files plus a transaction log (`_delta_log`)
    that makes table changes reliable.
-   **Bronze:** keep the incoming data with minimal changes.
-   **Silver:** clean and validate it.
-   **Gold:** organize it for business questions.
-   **Auto Loader:** watches a cloud-storage directory for files it has
    not processed before. Its checkpoint remembers progress.

------------------------------------------------------------------------

## 2. Understand the difference between external location, schema, and table

These are different objects; creating one does not automatically create
the others.

  ------------------------------------------------------------------------------
  Object                  Example                        What it does
  ----------------------- ------------------------------ -----------------------
  Storage account         `stdatabricksretailsit`        Azure storage resource

  Container               `source`, `bronze`, `silver`,  Top-level ADLS Gen2
                          `gold`                         container

  External location       `ext_retailsit_bronze`         Unity Catalog object
                                                         combining a cloud path
                                                         and a storage
                                                         credential

  Catalog                 `retailsit`                    Top-level namespace for
                                                         governed data objects

  Schema                  `retailsit.bronze`             Namespace for tables in
                                                         the Bronze layer

  Table                   `retailsit.bronze.customers`   Named table registered
                                                         in Unity Catalog

  Table storage path      a folder under the Bronze      Physical Delta data and
                          container                      `_delta_log`
  ------------------------------------------------------------------------------

### Crucial rule: "mapping" does not mean the same thing in every case

1.  **External location** grants governed access to a path. It is not a
    table and does not automatically register files.
2.  `CREATE SCHEMA ... MANAGED LOCATION 'abfss://...'` sets the default
    storage root for **managed tables** in that schema. It uses the
    path, not the external-location name.
3.  A managed table created with
    `saveAsTable("retailsit.bronze.customers")` is stored by Unity
    Catalog under the schema's managed storage location.
4.  An external table registered with
    `CREATE TABLE ... LOCATION 'abfss://...'` points to an existing
    table folder. The folder must already contain a valid Delta table if
    `USING DELTA` is specified.
5.  **Do not register a raw CSV folder as `USING DELTA`.** First read
    the CSV and write it as Delta, or use Auto Loader to do that.

For this learning project, the simplest route is: **use the external
locations for governed storage access, set schema managed locations,
then write managed Delta tables into those schemas.** This avoids
manually registering each table folder.

------------------------------------------------------------------------

## 3. Inspect Unity Catalog before changing anything

Run these commands in Databricks SQL Editor. Run one statement at a time
if the editor reports multiple-statement limitations.

### 3.1 See the current context

``` sql
SELECT current_catalog(), current_schema();
```

### 3.2 List catalogs, schemas, and tables

``` sql
SHOW CATALOGS;
SHOW SCHEMAS IN retailsit;
SHOW TABLES IN retailsit.bronze;
SHOW TABLES IN retailsit.silver;
SHOW TABLES IN retailsit.gold;
```

If the catalog does not exist yet, `SHOW SCHEMAS IN retailsit` will fail
until you create it.

### 3.3 List external locations

``` sql
SHOW EXTERNAL LOCATIONS;
```

Inspect each known external location:

``` sql
DESCRIBE EXTERNAL LOCATION ext_retailsit_bronze;
DESCRIBE EXTERNAL LOCATION ext_retailsit_silver;
DESCRIBE EXTERNAL LOCATION ext_retailsit_gold;
```

Check grants if needed:

``` sql
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_bronze;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_silver;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_gold;
```

You can inspect storage credentials with:

``` sql
SHOW STORAGE CREDENTIALS;
DESCRIBE STORAGE CREDENTIAL cred_adls_retail;
```

Use the credential name that actually exists in your workspace;
`cred_adls_retail` is only the expected example from the earlier project
setup.

### 3.4 What the URLs should look like

Based on the storage account name provided for this rebuild, the
expected paths are:

``` text
Source:  abfss://source@stdatabricksretailsit.dfs.core.windows.net/
Bronze:  abfss://bronze@stdatabricksretailsit.dfs.core.windows.net/
Silver:  abfss://silver@stdatabricksretailsit.dfs.core.windows.net/
Gold:    abfss://gold@stdatabricksretailsit.dfs.core.windows.net/
```

Verify the exact URLs in `DESCRIBE EXTERNAL LOCATION`. Do not assume an
external location has the expected URL just because its name sounds
correct.

------------------------------------------------------------------------

## 4. Create the catalog and schemas

First inspect the existing catalog and schemas. Do not drop existing
objects as a "cleanup" shortcut.

``` sql
CREATE CATALOG IF NOT EXISTS retailsit;
```

If the schemas do not exist, create them with their managed storage
roots:

``` sql
CREATE SCHEMA IF NOT EXISTS retailsit.bronze
MANAGED LOCATION 'abfss://bronze@stdatabricksretailsit.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retailsit.silver
MANAGED LOCATION 'abfss://silver@stdatabricksretailsit.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retailsit.gold
MANAGED LOCATION 'abfss://gold@stdatabricksretailsit.dfs.core.windows.net/';
```

**Important:** These statements do not change the managed location of a
schema that already exists. Check existing schema locations first:

``` sql
DESCRIBE SCHEMA EXTENDED retailsit.bronze;
DESCRIBE SCHEMA EXTENDED retailsit.silver;
DESCRIBE SCHEMA EXTENDED retailsit.gold;
```

Look for `Location` and `RootLocation`. If a schema already exists with
the wrong location, do not drop it blindly. Decide how to migrate or use
a new schema after inspecting its tables and data.

### Required permissions

The identity running these commands needs permission to create/use
catalog and schemas. The storage credential behind each external
location needs the relevant ADLS permissions. Schema creation or table
writes can fail if the identity lacks permission or if the managed
location is not covered by a valid external location.

------------------------------------------------------------------------

## 5. Check the source files

The four supplied CSV files have these columns and row counts:

  ------------------------------------------------------------------------
  File                                  Initial rows Columns
  --------------------- ---------------------------- ---------------------
  `customers.csv`                                 10 `CustomerID`,
                                                     `CustomerName`,
                                                     `Email`, `City`,
                                                     `State`,
                                                     `ModifiedDate`

  `products.csv`                                  10 `ProductID`,
                                                     `ProductName`,
                                                     `Category`,
                                                     `UnitPrice`,
                                                     `StockQuantity`,
                                                     `ModifiedDate`

  `orders.csv`                                    25 `OrderID`,
                                                     `CustomerID`,
                                                     `ProductID`,
                                                     `Quantity`,
                                                     `TotalAmount`,
                                                     `OrderDate`,
                                                     `ModifiedDate`

  `stores.csv`                                     7 `StoreID`,
                                                     `StoreName`, `City`,
                                                     `State`,
                                                     `ModifiedDate`
  ------------------------------------------------------------------------

The files are named in lowercase in the source set. ADLS paths are
case-sensitive in practice, so match the actual file and folder names.

### Recommended folder organization

For Auto Loader, keep each entity's files in its own directory, because
each entity has a different schema:

``` text
source/
  customers/customers.csv
  products/products.csv
  orders/orders.csv
  stores/stores.csv
```

For incremental tests, add new files to the corresponding folder, for
example:

``` text
source/orders/orders_2026-10-12.csv
```

If your files are currently all in the root of the `source` container,
move/copy them into the entity folders using Azure Storage Explorer or
the Azure portal. Do not put four different CSV schemas in one Auto
Loader directory.

------------------------------------------------------------------------

## 6. Verify the source paths from a Databricks Python notebook

Use a **Python notebook** for the following code (not the SQL Editor).

``` python
storage_account = "stdatabricksretailsit"
source_root = f"abfss://source@{storage_account}.dfs.core.windows.net/"

display(dbutils.fs.ls(source_root))
```

Inspect each entity folder:

``` python
for entity in ["customers", "products", "orders", "stores"]:
    path = f"{source_root}{entity}/"
    print(f"\n--- {entity} ---")
    display(dbutils.fs.ls(path))
```

If your files are directly in the source root rather than in subfolders,
this listing will show that. Organize the files first, or update the
Auto Loader input path to the correct entity-specific folder.

A folder's listed size can be zero even when it contains files; list the
contents inside the folder to verify.

------------------------------------------------------------------------

## 7. Bronze layer: initial and incremental file ingestion with Auto Loader

### What Auto Loader does

Auto Loader uses Spark's `cloudFiles` source to discover files in cloud
storage. It records progress in a **checkpoint** so a running query can
resume and avoid processing the same discovered file repeatedly.

Auto Loader does not automatically: - clean names or fix invalid
values; - remove duplicate business keys across different files; -
detect every changed row inside an old file and update a table by key; -
create Silver and Gold transformations for you.

For this project, use one Auto Loader stream per entity. Keep schema and
checkpoint paths separate for every entity, and do not delete
checkpoints between normal runs.

### 7.1 Create Bronze tables using Auto Loader

Run the following in a Python notebook attached to compute that supports
Auto Loader. This version processes currently available files and then
stops; it is convenient for a learning project and can be run again
after new files arrive.

> Before running, confirm that the source folders exist and that the
> Bronze schema is configured. The target tables below are managed
> tables registered in Unity Catalog. The schema's managed location
> determines their storage root.

``` python
from pyspark.sql import functions as F

storage_account = "stdatabricksretailsit"
source_root = f"abfss://source@{storage_account}.dfs.core.windows.net/"
bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

def ingest_csv_to_bronze(entity):
    source_path = f"{source_root}{entity}/"
    schema_path = f"{bronze_root}_autoloader/schema/{entity}/"
    checkpoint_path = f"{bronze_root}_autoloader/checkpoint/{entity}/"

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .option("inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"retailsit.bronze.{entity}")
    )

    query.awaitTermination()
    print(f"Finished available files for {entity}")

for entity in ["customers", "products", "orders", "stores"]:
    ingest_csv_to_bronze(entity)
```

**First-run note:** If one of the target tables already exists as an
empty or invalid registration, this code may fail while writing to it.
Do not drop it automatically. Inspect the table first; if it is a broken
empty registration and contains no needed data, carefully
remove/recreate that table registration before rerunning. Keep the
source files and external locations intact.

### 7.2 Verify Bronze table definitions and counts

In SQL Editor:

``` sql
SHOW TABLES IN retailsit.bronze;

DESCRIBE TABLE EXTENDED retailsit.bronze.customers;
DESCRIBE TABLE EXTENDED retailsit.bronze.products;
DESCRIBE TABLE EXTENDED retailsit.bronze.orders;
DESCRIBE TABLE EXTENDED retailsit.bronze.stores;

SELECT COUNT(*) FROM retailsit.bronze.customers;
SELECT COUNT(*) FROM retailsit.bronze.products;
SELECT COUNT(*) FROM retailsit.bronze.orders;
SELECT COUNT(*) FROM retailsit.bronze.stores;
```

For the initial sample data, expected counts are 10, 10, 25, and 7
respectively, assuming the files were ingested once and contain exactly
the supplied records.

### 7.3 How to test incremental file ingestion

1.  Leave the existing checkpoint paths unchanged.
2.  Add a **new file** to the matching source folder, for example
    `source/orders/orders_2026-10-12.csv`.
3.  Ensure it has the same header/schema as the existing orders CSV.
4.  Run the same Auto Loader notebook again.
5.  Auto Loader should discover the new file and append its rows to
    `retailsit.bronze.orders`.
6.  Check the count and the `_source_file` column.

``` sql
SELECT COUNT(*) FROM retailsit.bronze.orders;

SELECT _source_file, COUNT(*) AS rows_loaded
FROM retailsit.bronze.orders
GROUP BY _source_file
ORDER BY _source_file;
```

**Important incremental-load distinction:** Auto Loader's checkpoint
tracks files, not business changes to a record. If you replace an old
file in place or a customer's details change in a later file, Bronze may
contain multiple versions of that customer. Silver must apply a
deterministic deduplication or merge rule using a key and
`ModifiedDate`. Do not reset the checkpoint for a routine run.

------------------------------------------------------------------------

## 8. Silver layer: clean and deduplicate Bronze data

Silver reads Bronze Delta tables. It does not read the source CSV files
directly.

### 8.1 Customers

``` python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers_bronze = spark.table("retailsit.bronze.customers")

w = Window.partitionBy("CustomerID").orderBy(F.col("ModifiedDate").desc_nulls_last())

customers_silver = (
    customers_bronze
    .withColumn("CustomerName", F.trim(F.col("CustomerName")))
    .withColumn("Email", F.lower(F.trim(F.col("Email"))))
    .withColumn("City", F.initcap(F.trim(F.col("City"))))
    .withColumn("State", F.upper(F.trim(F.col("State"))))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("CustomerID").isNotNull())
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(customers_silver.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.silver.customers"))
```

### 8.2 Products

``` python
products_bronze = spark.table("retailsit.bronze.products")

w = Window.partitionBy("ProductID").orderBy(F.col("ModifiedDate").desc_nulls_last())

products_silver = (
    products_bronze
    .withColumn("ProductName", F.trim(F.col("ProductName")))
    .withColumn("Category", F.initcap(F.trim(F.col("Category"))))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("decimal(12,2)"))
    .withColumn("StockQuantity", F.col("StockQuantity").cast("int"))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("ProductID").isNotNull())
    .filter(F.col("UnitPrice").isNotNull() & (F.col("UnitPrice") >= 0))
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(products_silver.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.silver.products"))
```

### 8.3 Orders

``` python
orders_bronze = spark.table("retailsit.bronze.orders")

w = Window.partitionBy("OrderID").orderBy(F.col("ModifiedDate").desc_nulls_last())

orders_silver = (
    orders_bronze
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("TotalAmount", F.col("TotalAmount").cast("decimal(14,2)"))
    .withColumn("OrderDate", F.to_timestamp("OrderDate"))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("OrderID").isNotNull())
    .filter(F.col("Quantity").isNotNull() & (F.col("Quantity") > 0))
    .filter(F.col("TotalAmount").isNotNull() & (F.col("TotalAmount") >= 0))
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(orders_silver.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.silver.orders"))
```

### 8.4 Stores

``` python
stores_bronze = spark.table("retailsit.bronze.stores")

w = Window.partitionBy("StoreID").orderBy(F.col("ModifiedDate").desc_nulls_last())

stores_silver = (
    stores_bronze
    .withColumn("StoreName", F.trim(F.col("StoreName")))
    .withColumn("City", F.initcap(F.trim(F.col("City"))))
    .withColumn("State", F.upper(F.trim(F.col("State"))))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("StoreID").isNotNull())
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(stores_silver.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.silver.stores"))
```

### Why the Silver code uses a window

`dropDuplicates(["CustomerID"])` removes duplicates but does not
guarantee which version is kept. A window ordered by `ModifiedDate`
selects the latest record for each key. If two versions have the same
timestamp, add a deterministic tie-breaker for a production system.

The example overwrites each Silver table from the complete Bronze table.
This is easy to understand and rerun for a small learning project. A
production incremental Silver pipeline should use Delta `MERGE` or
another controlled upsert strategy.

------------------------------------------------------------------------

## 9. Gold layer: create business-ready tables

Gold should answer business questions. In this example we create a sales
fact table by joining Silver orders to products, customers and stores.
The supplied Orders columns do not include `StoreID`, so do not join
orders to stores without a valid business relationship. We can still
create a valid sales fact using orders, products and customers.

### 9.1 Gold sales detail

``` python
from pyspark.sql import functions as F

orders = spark.table("retailsit.silver.orders").alias("o")
customers = spark.table("retailsit.silver.customers").alias("c")
products = spark.table("retailsit.silver.products").alias("p")

gold_sales = (
    orders
    .join(customers, F.col("o.CustomerID") == F.col("c.CustomerID"), "left")
    .join(products, F.col("o.ProductID") == F.col("p.ProductID"), "left")
    .select(
        F.col("o.OrderID"),
        F.col("o.OrderDate"),
        F.col("o.CustomerID"),
        F.col("c.CustomerName"),
        F.col("o.ProductID"),
        F.col("p.ProductName"),
        F.col("p.Category"),
        F.col("o.Quantity"),
        F.col("o.TotalAmount")
    )
)

(gold_sales.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.gold.gold_sales"))
```

### 9.2 Gold daily sales summary

``` python
gold_daily_sales = (
    spark.table("retailsit.gold.gold_sales")
    .withColumn("OrderDate", F.to_date("OrderDate"))
    .groupBy("OrderDate")
    .agg(
        F.countDistinct("OrderID").alias("OrderCount"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("TotalAmount").alias("SalesAmount")
    )
    .orderBy("OrderDate")
)

(gold_daily_sales.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.gold.gold_daily_sales"))

display(spark.table("retailsit.gold.gold_daily_sales"))
```

### 9.3 Gold category summary

``` python
gold_category_sales = (
    spark.table("retailsit.gold.gold_sales")
    .groupBy("Category")
    .agg(
        F.countDistinct("OrderID").alias("OrderCount"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("TotalAmount").alias("SalesAmount")
    )
)

(gold_category_sales.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("retailsit.gold.gold_category_sales"))
```

------------------------------------------------------------------------

## 10. SQL commands for checking schemas, table definitions, paths and data

### 10.1 Schema definitions and managed locations

``` sql
DESCRIBE SCHEMA EXTENDED retailsit.bronze;
DESCRIBE SCHEMA EXTENDED retailsit.silver;
DESCRIBE SCHEMA EXTENDED retailsit.gold;
```

Look for `Location`, `RootLocation`, `Owner`, and other metadata.

### 10.2 Table columns and data types

``` sql
DESCRIBE TABLE retailsit.bronze.customers;
DESCRIBE TABLE retailsit.silver.customers;
DESCRIBE TABLE retailsit.gold.gold_sales;
```

### 10.3 Detailed table metadata and location

``` sql
DESCRIBE TABLE EXTENDED retailsit.bronze.customers;
DESCRIBE TABLE EXTENDED retailsit.silver.customers;
DESCRIBE TABLE EXTENDED retailsit.gold.gold_sales;
```

Where supported, the Delta detail command gives structured location
information:

``` sql
DESCRIBE DETAIL retailsit.bronze.customers;
DESCRIBE DETAIL retailsit.silver.customers;
DESCRIBE DETAIL retailsit.gold.gold_sales;
```

### 10.4 Row counts

``` sql
SELECT COUNT(*) AS customer_count FROM retailsit.bronze.customers;
SELECT COUNT(*) AS product_count  FROM retailsit.bronze.products;
SELECT COUNT(*) AS order_count    FROM retailsit.bronze.orders;
SELECT COUNT(*) AS store_count    FROM retailsit.bronze.stores;

SELECT COUNT(*) AS customer_count FROM retailsit.silver.customers;
SELECT COUNT(*) AS product_count  FROM retailsit.silver.products;
SELECT COUNT(*) AS order_count    FROM retailsit.silver.orders;
SELECT COUNT(*) AS store_count    FROM retailsit.silver.stores;

SELECT COUNT(*) AS sales_rows FROM retailsit.gold.gold_sales;
```

### 10.5 Validate Gold output

``` sql
SELECT * FROM retailsit.gold.gold_sales LIMIT 10;
SELECT * FROM retailsit.gold.gold_daily_sales ORDER BY OrderDate;
SELECT * FROM retailsit.gold.gold_category_sales ORDER BY SalesAmount DESC;
```

### 10.6 Check Delta history

``` sql
DESCRIBE HISTORY retailsit.bronze.customers;
DESCRIBE HISTORY retailsit.silver.customers;
DESCRIBE HISTORY retailsit.gold.gold_sales;
```

### 10.7 Check Unity Catalog grants

``` sql
SHOW GRANTS ON SCHEMA retailsit.bronze;
SHOW GRANTS ON TABLE retailsit.bronze.customers;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_bronze;
```

Repeat with the appropriate object names. You need the appropriate
privileges to see or modify grants.

------------------------------------------------------------------------

## 11. Common errors and what they mean

### `TABLE_OR_VIEW_NOT_FOUND`

The table name, catalog, or schema is wrong, or the table has not been
registered. Check:

``` sql
SHOW CATALOGS;
SHOW SCHEMAS IN retailsit;
SHOW TABLES IN retailsit.bronze;
```

### `DELTA_READ_TABLE_WITHOUT_COLUMNS`

The registered table points to a Delta location without a usable schema,
or to the wrong folder. Test the physical folder directly:

``` python
path = "abfss://bronze@stdatabricksretailsit.dfs.core.windows.net/Customers/"
df = spark.read.format("delta").load(path)
df.printSchema()
display(df.limit(5))
```

If direct reading works but the registered table does not, inspect its
`LOCATION` with `DESCRIBE TABLE EXTENDED`. Do not immediately drop the
table or delete its data.

### `UNRESOLVED_COLUMN`

The code refers to a column name that does not exist. Inspect the real
schema:

``` python
spark.table("retailsit.bronze.products").printSchema()
```

The source column is `UnitPrice`, not `Price`, in the supplied products
file. Use exact column names.

### Access denied / permission errors

Check the external location URL, its storage credential, Unity Catalog
grants, and Azure RBAC permissions. Avoid granting broad permissions
unnecessarily.

### Auto Loader appears to load no new data

Check that: - a genuinely new file was added under the entity's source
folder; - the file has the expected header and format; - the same
persistent checkpoint path is used; - the query completed
successfully; - you are checking the correct target table.

Do not delete the checkpoint as a routine troubleshooting step.

------------------------------------------------------------------------

## 12. Final validation checklist

-   [ ] External locations exist and their URLs point to the correct
    containers.
-   [ ] Catalog `retailsit` and schemas `bronze`, `silver`, `gold`
    exist.
-   [ ] Source files are organized into entity-specific directories.
-   [ ] Auto Loader reads each entity folder and writes Bronze Delta
    tables.
-   [ ] Bronze initial counts match the supplied files: customers 10,
    products 10, orders 25, stores 7.
-   [ ] Adding a new file and rerunning with the same checkpoint adds
    its records to Bronze.
-   [ ] Silver tables have the correct columns and latest record per
    business key.
-   [ ] Gold sales detail and summaries query successfully.
-   [ ] Table locations and definitions have been checked with
    `DESCRIBE TABLE EXTENDED` / `DESCRIBE DETAIL`.
-   [ ] No source files or storage folders were deleted as part of
    troubleshooting.

## 13. Recommended teaching sequence

Teach one idea at a time:

1.  **Day 1:** Catalog, schema, table, external location and storage
    path.
2.  **Day 2:** Read a CSV in PySpark and inspect its schema.
3.  **Day 3:** Write a CSV as a Bronze Delta table.
4.  **Day 4:** Use Auto Loader and explain checkpoints by adding a new
    file.
5.  **Day 5:** Clean customers and products into Silver.
6.  **Day 6:** Clean orders and stores into Silver.
7.  **Day 7:** Join Silver tables and create Gold sales summaries.
8.  **Day 8:** Validate counts, table definitions, locations, Delta
    history and permissions.

**Core idea to remember:** An external location controls governed access
to cloud storage; a schema organizes tables; a Delta table stores
structured data; Auto Loader incrementally discovers files; Silver
cleans records; Gold prepares business outputs.
