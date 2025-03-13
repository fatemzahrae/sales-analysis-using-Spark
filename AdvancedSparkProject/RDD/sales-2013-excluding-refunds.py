from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sales2013ExcludingRefunds").getOrCreate()

sales_rdd = spark.sparkContext.textFile("dataset/Sales.txt").map(lambda x: x.split("|"))
refund_rdd = spark.sparkContext.textFile("dataset/Refund.txt").map(lambda x: x.split("|"))

# Extract transaction IDs (txID) from the Refund dataset
refund_tx_ids = refund_rdd.map(lambda x: x[1]).collect() 

# Broadcast the refund transaction IDs 
refund_tx_ids_broadcast = spark.sparkContext.broadcast(set(refund_tx_ids))


def is_2013_sale(row):
    try:
        return row[3].split(" ")[0].split("/")[-1] == "2013"  
    except IndexError:
        return False 


def is_refund(row):
    return row[0] in refund_tx_ids_broadcast.value 


# Filter sales for 2013 and exclude refunds
sales_2013_excluding_refunds_rdd = sales_rdd.filter(is_2013_sale) \
                                           .filter(lambda x: not is_refund(x)) \
                                           .map(lambda x: int(x[4]))  


total_sales_2013_excluding_refunds = sales_2013_excluding_refunds_rdd.reduce(lambda a, b: a + b)


print("Total Sales in 2013 (excluding refunds):", total_sales_2013_excluding_refunds)
spark.stop()