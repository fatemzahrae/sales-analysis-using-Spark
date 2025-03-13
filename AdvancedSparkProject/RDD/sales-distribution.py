from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()


product_rdd = spark.sparkContext.textFile("dataset/Product.txt").map(lambda x: x.split("|"))
sales_rdd = spark.sparkContext.textFile("dataset/Sales.txt").map(lambda x: x.split("|"))

# Extract ProductID, Amount, and Quantity  
sales_distribution_rdd = sales_rdd.map(lambda x: (x[2], (int(x[5]), int(x[4])))) \
                                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))


# Join with Product data to get Product Name
product_rdd_mapped = product_rdd.map(lambda x: (x[0], x[1]))  # (ProductID, ProductName)

sales_distribution_rdd = sales_distribution_rdd.join(product_rdd_mapped) \
                                               .map(lambda x: (x[1][1], x[1][0][0], x[1][0][1]))  # (ProductName, Quantity, Amount)

result = sales_distribution_rdd.collect()

print("Here are the amount and the quantity for each product")
for row in result:
    print(row)
