# Databricks notebook source
# MAGIC %md
# MAGIC ### bronze_transform_tcibd001
# MAGIC
# MAGIC - create: **`08 may 2025`** | modify: **`08 may 2025`** 
# MAGIC - version: **`1.0`** | by: **`Rommel Bojorge 👨🏻‍💻`**

# COMMAND ----------

# MAGIC %md
# MAGIC >**`help`**
# MAGIC - sqlf: alias for the module functions of pyspark.sql
# MAGIC - sdf_: prefix for any (s)park (d)ata (f)rame

# COMMAND ----------

# MAGIC %md
# MAGIC ### load libraries

# COMMAND ----------

from pyspark.sql import functions as sqlf

# COMMAND ----------

# MAGIC %md
# MAGIC ### load table (E)TL

# COMMAND ----------

# tc: common
# ibd: item base data
# 001: items
sdf_tcibd001 = spark.read.table("bronze.ln_cloud.tcibd001")
print(sdf_tcibd001.count())

# COMMAND ----------

# print dataframe schema
sdf_tcibd003.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA (Exploratory Data Analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC #### cheking if there are null values

# COMMAND ----------

# save dataframe metadata for use in the for loop ...
#   ... and examine is there a column with null value

sdf = sdf_tcibd003
null_columns = sdf_tcibd003.columns

print(f"The df contains {sdf.count()} rows")

for col in null_columns:
    null_count = sdf.filter(sqlf.isnull(col)).count()
    if null_count > 0:
        print(f"The column '{col}' contains {null_count} null values")


# COMMAND ----------

# MAGIC %md
# MAGIC #### review distinct values

# COMMAND ----------

sdf = sdf_tcibd003

# Count the number of distinct values in a column
print(sdf.select(sqlf.trim(sqlf.col("item"))).distinct().count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### transform table E(T)L

# COMMAND ----------

# examine the dataframe
# display(sdf_tcibd003.select("*").limit(5))

sdf_tcibd003_v1 = (
    sdf_tcibd003
    # transformations
    .withColumn("articulo", sqlf.trim(sqlf.col("item"))) # remove white space for column "item"
    .withColumn("aprobado", sqlf.when(sqlf.col("appr") == 1, "Si").otherwise("No")) # appr = 1 -> Si | = 2 -> No
    # select
    .select(
        sqlf.col("compnr").alias("cia")
        ,sqlf.col("articulo")
        ,sqlf.col("citg").alias("grupo_articulo")
        ,sqlf.col("basu").alias("unidad_base")
        ,sqlf.col("unit").alias("unidad")
        ,sqlf.col("conv").alias("factor_conversion")
        ,sqlf.col("rpow").alias("elevado_10")
        ,sqlf.col("aprobado")
    )
).show(5)

