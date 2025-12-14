# simple_tcp_streaming.py
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: simple_tcp_streaming.py <host> <port>")
        sys.exit(1)
    
    host = sys.argv[1]
    port = sys.argv[2]
    
    spark = SparkSession.builder \
        .appName("SimpleTCPStream") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    # Print raw messages
    print("Starting to listen for messages...")
    
    query = lines.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination()