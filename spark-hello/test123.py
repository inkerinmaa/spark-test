from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import time

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("PythonSparkTest") \
        .getOrCreate()
    
    # Simple test
    data = [("Hello", "World"), ("Spark", "Kubernetes")]
    schema = StructType([
        StructField("word1", StringType(), True),
        StructField("word2", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df.show()
    
    # Count rows
    count = df.count()
    print(f"Total rows: {count}")
    
    # Create a simple transformation
    df.selectExpr("concat(word1, ' ', word2) as sentence").show()
    
    spark.stop()

if __name__ == "__main__":
    main()