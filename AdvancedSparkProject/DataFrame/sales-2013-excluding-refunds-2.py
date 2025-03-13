from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

spark = SparkSession.builder \
    .appName("Sales2013ExcludingRefunds") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

sales_df = spark.read.option("delimiter", "|").csv("dataset/Sales.txt", inferSchema=True, header=False)

sales_df = sales_df.select(
    col("_c0").alias("SaleID"),
    col("_c1").alias("CustomerID"),
    col("_c2").alias("ProductID"),
    col("_c3").alias("SaleDate"),
    col("_c4").alias("Amount"),
    col("_c5").alias("Quantity")
)

refund_df = spark.read.option("delimiter", "|").csv("dataset/Refund.txt", inferSchema=True, header=False)

# Extract transaction IDs (txID) from Refund dataset
refund_tx_ids = refund_df.select("_c1").rdd.flatMap(lambda x: x).collect()

# Convert SaleDate to DateType 
sales_df = sales_df.withColumn("SaleDate", to_date(col("SaleDate"), "MM/dd/yyyy HH:mm:ss"))


sales_2013_df = sales_df.filter(year(col("SaleDate")) == 2013)

# Filter out refunds using a list of transaction IDs
sales_2013_excluding_refunds_df = sales_2013_df.filter(~col("SaleID").isin(refund_tx_ids))

total_sales_2013_excluding_refunds = sales_2013_excluding_refunds_df.agg({"Amount": "sum"}).collect()[0][0]

print("Total Sales in 2013 (excluding refunds):", total_sales_2013_excluding_refunds)

spark.stop()
