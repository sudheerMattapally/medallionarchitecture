# Databricks notebook source
customer_bronze=spark.read\
    .option("header","true")\
    .option("inferschema","true")\
    .csv("/FileStore/tables/customer.csv")
display(customer_bronze)    

# COMMAND ----------

orders_bronze=spark.read\
    .option("header","true")\
    .option("inferschema","true")\
    .csv("/FileStore/tables/orders.csv")
display(orders_bronze)   

# COMMAND ----------

customer_bronze.write.mode("overwrite").format("parquet").save("bronze/customers")

# COMMAND ----------

orders_bronze.write.mode("overwrite").format("parquet").save("bronze/orders")

# COMMAND ----------

#read bronze data
customer=spark.read.parquet("dbfs:/bronze/customers")
orders=spark.read.parquet("dbfs:/bronze/orders")

# COMMAND ----------

display(orders)

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# clean orders data

from pyspark.sql.functions import *
orders_cleaned=orders.filter(col("status")!="cancelled")\
    .withColumn("order_date",to_date("order_date","yyyy-MM-dd"))\
    .withColumn("amount",col("amount").cast("float"))\
    .withColumn("customer_id",col("customer_id").cast(("int")))
display(orders_cleaned)

# COMMAND ----------


display(customer)


# COMMAND ----------

# # cleaned customer data 
customer_cleaned=customer.withColumn("customer_id",col("customer_id").cast("int"))\
    .withColumn("signup_date",to_date("signup_date","yyyy-MM-dd"))

# COMMAND ----------

display(customer_cleaned)

# COMMAND ----------

customer_orders=customer_cleaned.join(orders_cleaned,on="customer_id",how="inner")\
  .select("order_id","customer_id","first_name","last_name","email","order_date","amount")
display(customer_orders)  

# COMMAND ----------

customer_cleaned.write.mode("overwrite").parquet("silver/customer")
orders_cleaned.write.mode("overwrite").parquet("silver/orders")
customer_orders.write.mode("overwrite").parquet("silver/customer_orders")

# COMMAND ----------

# gold layer
customer_gold=spark.read.parquet("dbfs:/silver/customer_orders")
display(customer_gold)

# COMMAND ----------

display(customer_gold)

# COMMAND ----------

customer_final=customer_gold.groupBy("customer_id")\
    .agg(sum("amount").alias("totalAmount"))

                                                            
                                                            
                                                            
                                        

# COMMAND ----------

display(customer_final)

# COMMAND ----------

customer_final.write.mode("overwrite").parquet("gold/customer_final")

# COMMAND ----------

# MAGIC %md
# MAGIC