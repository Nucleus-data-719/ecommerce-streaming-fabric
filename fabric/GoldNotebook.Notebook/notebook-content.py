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

from pyspark.sql import functions as F

silver_df = spark.read.table("silver_table")

if spark.catalog.tableExists("gold_processed_events"):
    processed_ids = (
        spark.read.table("gold_processed_events").select("event_id")
    )

    new_data = silver_df.join(
        processed_ids,
        on="event_id",
        how="left_anti"
    )
else:
    new_data=silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_data = new_data.drop_duplicates(["event_id"])

new_count = new_data.count()
print(f"New records to process: {new_count}")

if new_count == 0:
    print("No new events.")
else:
    display(new_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

purchases = new_data.filter(F.col("event_type") == "purchase")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Sales Summary**

# CELL ********************

gold_sales_summary = (
    purchases.groupBy("event_date", "currency")
    .agg(
        F.count("event_id").alias("total_purchases"),
        F.sum("payment_amount").alias("total_revenue"),
        F.avg("payment_amount").alias("average_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy("event_date", "currency")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Product Performance**

# CELL ********************

gold_product_performance = (
    purchases.groupBy("product_id", "product_name", "category", "currency")
    .agg(
        F.count("event_id").alias("total_orders"),
        F.sum("payment_amount").alias("total_revenue"),
        F.avg("payment_amount").alias("average_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("total_revenue").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold City Performance**

# CELL ********************

gold_city_performance = (
    purchases.groupBy("product_name","city","currency")
    .agg(
        F.count("event_id").alias("total_purchases"),
        F.sum("payment_amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("total_revenue").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Event Summary**

# CELL ********************

gold_event_summary = (
    new_data.groupBy("event_date", "event_type")
    .agg(
        F.count("event_id").alias("total_events"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy("event_date", "event_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Currency Summary**

# CELL ********************

gold_currency_summary = (
    purchases.groupBy("currency")
    .agg(
        F.count("event_id").alias("total_purchases"),
        F.sum("payment_amount").alias("total_revenue"),
        F.avg("payment_amount").alias("average_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy(F.col("total_purchases").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create or append table**

# CELL ********************

def append_or_create(df, table_name):
    if df.limit(1).count() > 0:
        if spark.catalog.tableExists(table_name):
            df.write.mode("append").saveAsTable(table_name)
        else:
            df.write.mode("overwrite").saveAsTable(table_name)
        print(f"Saved {table_name}")
    else:
        print(f"No rows to write to  {table_name} in this batch")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

append_or_create(gold_sales_summary, "gold_sales_summary")
append_or_create(gold_product_performance, "gold_product_performance")
append_or_create(gold_city_performance, "gold_city_performance")
append_or_create(gold_event_summary, "gold_event_summary")
append_or_create(gold_currency_summary, "gold_currency_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Update Checkpoint**

# CELL ********************

new_processed_ids = new_data.select("event_id")

if spark.catalog.tableExists("gold_processed_events"):
    new_processed_ids.write.mode("append").saveAsTable("gold_processed_events")
else:
    new_processed_ids.write.mode("overwrite").saveAsTable("gold_processed_events")

print(f"Checkpoint updated with {new_count} event IDs.")
print("Incremental Gold Load completed.")

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
