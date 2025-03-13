from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("userProduct").getOrCreate()

customer_df = spark.read.option("delimiter", "|").csv("dataset/Customer.txt", inferSchema=True, header=False)

customer_df = customer_df.select(
    col("_c0").alias("custID"),
    col("_c1").alias("firstName"),
    col("_c2").alias("lastName")
)

sales_df = spark.read.option("delimiter", "|").csv("dataset/Sales.txt", inferSchema=True, header=False)

sales_df = sales_df.select(
    col("_c0").alias("SaleID"),
    col("_c1").alias("custID"),
    col("_c2").alias("prodID"),
    col("_c3").alias("SaleDate"),
    col("_c4").alias("Amount"),
    col("_c5").alias("Quantity")
)

# Join Sales with Customer on custID
sales_customer_joined_df = sales_df.join(customer_df, on="custID")

# Group by custID and calculate total quantity of products purchased by each customer
user_products_qty_df = sales_customer_joined_df.groupBy("custID", "firstName", "lastName") \
    .agg(sum("Quantity").alias("total_quantity"))

user_products_qty_df.show(truncate=False)

spark.stop()
