# sales-analysis-using-Spark
A PySpark-based project for analyzing sales data, using both RDDs and DataFrames for processing and analyzing large datasets.

## Key Features
- **Sales Distribution**: Calculate the total amount and quantity sold for each product.
- **Sales by Year**: Analyze total sales for a specific year (e.g., 2013), excluding refunds.
- **Customer-Product Insights**: Determine the total number of products purchased by each customer.
- **RDD and DataFrame Implementations**: Includes both RDD and DataFrame versions for comparison and learning purposes.


## Technologies Used
- **Apache Spark**: For distributed data processing.
- **PySpark**: Python API for Spark.

## How to Run
you can run a script using the command
spark-submit --master spark://your-master-url rdd/sales_distribution.py
