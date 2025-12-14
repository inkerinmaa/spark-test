from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("hello-world").getOrCreate()

    print("Hello from PySpark running on Kubernetes via Spark Operator!")
    print("Spark version:", spark.version)

    # tiny action to prove the job actually ran
    n = spark.range(1, 6).count()
    print("Count(range(1,6)) =", n)

    spark.stop()
