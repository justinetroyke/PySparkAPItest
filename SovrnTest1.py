# table_a:
# timestamp 
# sensor_id 
# temperature 
# humidity 
# pressure

# table_b:
# timestamp 
# sensor_id 
# wind_speed 
# wind_direction 
# precipitation

# include example output from the spark job as well as the code used
# to get that output.

# Join on sensor_id (a & b)
# output a list of
# unique entries based on 
# "sensor_id" 
# that contain
# "temperature", 
# "humidity", 
# "precipitation" (b) 
# ordered by "timestamp"

## Based on tables my main considerations are 
## to optimize performance and handle the scale of the data efficiently 

# 1 - cluster size resources 
    # Consider memory (for broadcast variables), CPU, cluster & data size and complexity of job 

# 2 - Consider data distribution 
#   partitioning with thought to joins  
#   patition size 
#   avoid skewness  

#3 - Choose appropriate join strategy 
    # don't want to cache
    # JOIN and order by timestamp A then timestamp B where timestamp_a != timestamp_b

#4 -  After implementation, resource tune from there 


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinWeatherData").getOrCreate()

# Load tables
table_a = spark.read.parquet("path/to/table_a")
table_b = spark.read.parquet("path/to/table_b")

# Perform the join
sql_query = """
                    SELECT 
                    a.timestamp, 
                    a.sensor_id, 
                    a.temperature, 
                    a.humidity, 
                    b.precipitation
                    FROM table_a a
                    JOIN table_b b ON a.sensor_id = b.sensor_id
                    ORDER BY a.timestamp, b.timestamp
"""
result = spark.sql(sql_query)

# This just prints but you could write to a file location here 
print(result)

# Stop the Spark session
spark.stop()