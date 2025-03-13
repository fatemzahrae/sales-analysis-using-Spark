from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("userProduct").getOrCreate()

customer_rdd = spark.sparkContext.textFile("dataset/Customer.txt").map(lambda x: x.split("|"))
sales_rdd = spark.sparkContext.textFile("dataset/Sales.txt").map(lambda x: x.split("|"))


# Extract columns from Sales RDD: (custID, prodID, quantity)
sales_cust_prod_qty_rdd = sales_rdd.map(lambda x: (x[1], (x[2], int(x[5]))))  # custID at index 1, prodID at index 2, quantity at index 5

# Extract columns from Customer RDD: (custID, firstName, lastName)
customer_info_rdd = customer_rdd.map(lambda x: (x[0], (x[1], x[2])))  # custID at index 0

# Join Sales with Customer to associate sales with customers
# Result: (custID, ((prodID, quantity), (firstName, lastName)))
sales_customer_joined_rdd = sales_cust_prod_qty_rdd.join(customer_info_rdd)

# Group by custID and sum the quantities of products for each customer
# Result: (custID, (firstName, lastName, total_quantity))
user_products_qty_rdd = sales_customer_joined_rdd.map(lambda x: (x[0], (x[1][1][0], x[1][1][1], x[1][0][1]))) \
                                                .reduceByKey(lambda a, b: (a[0], a[1], a[2] + b[2]))

results = user_products_qty_rdd.collect()
for result in results:
    print(f"Customer ID: {result[0]}, Name: {result[1][0]} {result[1][1]}, Total Quantity: {result[1][2]}")

spark.stop()