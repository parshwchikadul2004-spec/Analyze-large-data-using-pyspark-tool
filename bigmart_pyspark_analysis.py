from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigMart Sales Big Data Analysis") \
    .getOrCreate()


df = spark.read.csv(
    "/mnt/data/BigMart_Sales_Cleaned (1).csv",
    header=True,
    inferSchema=True
)

df.show(5)


df.printSchema()
df.describe().show()

print("Total Rows:", df.count())

df.groupBy("Item_Type").count().show()

df.groupBy("Outlet_Type").sum("Item_Outlet_Sales").show()

df.filter(df.Item_Outlet_Sales > 2000).show(5)

df.orderBy(df.Item_Outlet_Sales.desc()).show(5)

print("Number of Partitions:", df.rdd.getNumPartitions())

spark.stop()
