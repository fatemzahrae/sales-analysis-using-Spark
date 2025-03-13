from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesAmount").getOrCreate()

sales_rdd = spark.sparkContext.textFile("dataset/Sales.txt").map(lambda x: x.split("|"))

def is_2013_sale(row):
    try:
        return row[3].split(" ")[0].split("/")[-1] == "2013"
    except IndexError:
        return False  

sales_2013_rdd = sales_rdd.filter(is_2013_sale) \
                          .map(lambda x: int(x[4])) \
                          .reduce(lambda a, b: a + b)

print("Total Sales in 2013:", sales_2013_rdd)


