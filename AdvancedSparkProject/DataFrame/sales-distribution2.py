from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

product_df = spark.read.option("delimiter", "|").csv("dataset/Product.txt", inferSchema=True, header=False)
sales_df = spark.read.option("delimiter", "|").csv("dataset/Sales.txt", inferSchema=True, header=False)

product_df = product_df.select(
    col("_c0").alias("ProductID"),
    col("_c1").alias("ProductName"),
    col("_c2").alias("ProductType"),
    col("_c3").alias("ProductVersion"),
    col("_c4").alias("Price")
)

sales_df = sales_df.select(
    col("_c0").alias("SaleID"),
    col("_c1").alias("CustomerID"),
    col("_c2").alias("ProductID"),
    col("_c3").alias("SaleDate"),
    col("_c4").alias("Amount"),
    col("_c5").alias("Quantity")
)


sales_df = sales_df.withColumn("Amount", col("Amount").cast("int")) \
                   .withColumn("Quantity", col("Quantity").cast("int"))

# Group by ProductID to calculate total Quantity and Amount
sales_distribution_df = sales_df.groupBy("ProductID").agg(
    sum("Quantity").alias("TotalQuantity"),
    sum("Amount").alias("TotalAmount")
)

# Join the sales distribution data with product data to get Product Name
result_df = sales_distribution_df.join(product_df, "ProductID").select(
    "ProductName", "TotalQuantity", "TotalAmount"
)

print("Here are the amount and the quantity for each product:")
result_df.show(truncate=False)
