from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
import datetime

conf = SparkConf()
conf.setAppName("nyctaxi")
conf.set("spark.executor.memory", "2g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.cores", "5")
spark = SparkContext(conf=conf)

for f in glob.glob("/data/nyctaxi/set1/*.parquet"):
    print(f)
    df = ps.read_parquet(f)
    print(df.columns)

    print(df.info(verbose=True))

#mean = df['fare_amount'].mean()
#print(mean)

print(df.groupby('payment_type')['fare_amount'].mean())

# Average ratio of trip cost that is tolls
# Total num of trips per month across all years
# Average price per mile, excluding tolls and mta taxes
# Most popular pickup/dropoff locations (use lat/long but rounded to 3 decimal places)

