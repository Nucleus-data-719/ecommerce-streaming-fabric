# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "89dc2f03-a59d-48b6-bf48-49305ec9824f",
# META       "default_lakehouse_name": "ecommerce_lakehouse",
# META       "default_lakehouse_workspace_id": "29f3c541-8fca-4dda-8810-654b86292e75",
# META       "known_lakehouses": [
# META         {
# META           "id": "89dc2f03-a59d-48b6-bf48-49305ec9824f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col

bronze_data = spark.read.table("bronze_table")

if spark.catalog.tableExists("silver_table"):
    existing_silver = spark.read.table("silver_table")
else:
    existing_silver = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if existing_silver is None:
    bronze_df = bronze_data
else:
    existing_ids = existing_silver.select("event_id").distinct()

    bronze_df = bronze_data.join(
        existing_ids,
        on="event_id",
        how="left_anti"
    )
print("New records:", bronze_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = bronze_df.select(
    col("event_id"),
    col("event_type"),
    col("timestamp"),

    col("customer.customer_id").alias("customer_id"),
    col("customer.city").alias("city"),

    col("product.product_id").alias("product_id"),
    col("product.name").alias("product_name"),
    col("product.category").alias("category"),
    col("product.price").cast("int").alias("price"),

    col("payment_method"),
    col("payment_amount"),
    col("currency"),
)

display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum, when

null_count = silver_df.select([
    sum(
        when(col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in silver_df.columns
])

display(null_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = silver_df.dropna(
    subset=[
        "event_id",
        "event_type",
        "timestamp",
        "customer_id",
        "product_id"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date

silver_df = silver_df.withColumn(
    "event_date",
    to_date(col("timestamp"))
)

display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.write.mode("append").saveAsTable("silver_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ecommerce_lakehouse.dbo.silver_table LIMIT 100")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
