from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, expr
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import logging
import os
import time
import sys

# Set up logging with clear formatting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Define GCS bucket name and BigQuery table info
GCS_BUCKET = "saturntest"  # Replace with your actual GCS bucket name
PROJECT_ID = "round-gamma-455617-f3"
DATASET = "trafficdata"
TABLE = "realtime_traffic"

# Wait for Kafka to be available
logger.info("Waiting for Kafka to be fully available...")
time.sleep(30)  # Initial wait

# Create the Spark session
logger.info("Creating Spark session...")
spark = SparkSession.builder \
    .appName("TrafficSparkStreamingApp") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints/traffic") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/gcp/key.json") \
    .getOrCreate()

# Set GCP credentials environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/gcp/key.json"

# Log information about the Spark session
logger.info(f"Spark version: {spark.version}")
logger.info(f"Spark master: {spark.sparkContext.master}")
logger.info("Spark session created successfully")

# List available packages for debugging
try:
    logger.info("Listing available JARs in the Spark classpath...")
    classpath = spark.sparkContext._jvm.System.getProperty("java.class.path")
    logger.info(f"Classpath contains: {classpath}")
    
    # Explicitly check for Kafka
    available_packages = spark.sparkContext._jvm.scala.collection.JavaConverters.asJavaCollection(
        spark.sparkContext._jsc.sc().listJars()
    )
    kafka_jars = [jar for jar in available_packages if "kafka" in jar.lower()]
    logger.info(f"Found Kafka-related JARs: {kafka_jars}")
    
    # Explicitly check for BigQuery
    bigquery_jars = [jar for jar in available_packages if "bigquery" in jar.lower()]
    logger.info(f"Found BigQuery-related JARs: {bigquery_jars}")
except Exception as e:
    logger.warning(f"Could not list JARs: {e}")

# Define schema for the JSON data
schema = StructType() \
    .add("location", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("timestamp", DoubleType())

# Test creating a simple DataFrame first to verify Spark is working
logger.info("Creating test DataFrame to verify Spark is working properly...")
test_data = [("test_location", 100, time.time())]
columns = ["location", "vehicle_count", "timestamp"]
test_df = spark.createDataFrame(test_data, columns)
test_df.show()
logger.info("Test DataFrame created successfully")

# Now try to connect to Kafka
try:
    logger.info("Setting up Kafka stream reader...")
    
    # Try to read a small batch from Kafka to test the connection
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "traffic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("Successfully created Kafka stream reader")
    
    # Parse the JSON data
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    
    # Add a readable timestamp column
    parsed_df = parsed_df.withColumn("timestamp_readable", from_unixtime(col("timestamp")))
    
    # Filter to get only records with vehicle_count < 250
    filtered_df = parsed_df.filter(col("vehicle_count") < 250)
    
    # Print schema for debugging
    logger.info("Kafka stream schema:")
    filtered_df.printSchema()
    
    # Output to console for testing
    logger.info("Starting console output for testing...")
    console_query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Wait for the console output to be ready before trying BigQuery
    logger.info("Console output started successfully, waiting to see some data...")
    time.sleep(30)
    
    # Try to also write to BigQuery using foreachBatch instead of direct streaming
    try:
        logger.info("Setting up BigQuery writer using foreachBatch...")
        table_path = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        logger.info(f"Target BigQuery table: {table_path}")
        
        # Define a function to process each batch
        def process_batch(batch_df, batch_id):
            if not batch_df.isEmpty():
                logger.info(f"Writing batch {batch_id} with {batch_df.count()} records to BigQuery")
                batch_df.write \
                    .format("bigquery") \
                    .option("table", table_path) \
                    .option("temporaryGcsBucket", GCS_BUCKET) \
                    .option("credentialsFile", "/gcp/key.json") \
                    .mode("append") \
                    .save()
                logger.info(f"Successfully wrote batch {batch_id} to BigQuery")
        
        # Use foreachBatch to process each micro-batch
        bq_query = filtered_df.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", "/app/checkpoints/traffic/bq_checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        logger.info("Successfully started BigQuery writer using foreachBatch")
        
        # Wait for termination of either stream
        spark.streams.awaitAnyTermination()
        
    except Exception as bq_err:
        logger.error(f"Error setting up BigQuery writer: {bq_err}")
        logger.error("BigQuery error details:", exc_info=True)
        logger.info("Continuing with console output only")
        console_query.awaitTermination()
        
except Exception as e:
    logger.error(f"Error in Kafka streaming setup: {e}")
    logger.error("Stack trace:", exc_info=True)
    
    # Enhanced error reporting
    try:
        # Log available stream sources
        available_sources = spark._jsparkSession.streams().sources()
        logger.info(f"Available stream sources: {list(available_sources)}")
        
        # Attempt to list available packages
        jars = spark.sparkContext._jsc.sc().listJars()
        jar_list = [j for j in spark.sparkContext._jvm.scala.collection.JavaConverters.asJavaCollection(jars)]
        logger.info(f"Available JARs: {jar_list}")
    except Exception as debug_err:
        logger.error(f"Error while getting debug info: {debug_err}")
    
    # Keep the application running for troubleshooting
    logger.info("Keeping application alive for troubleshooting...")
    while True:
        time.sleep(60)
        logger.info("Still running - check logs for errors")
        
finally:
    # This should only run when the streaming queries are stopped
    logger.info("Stopping Spark session")
    spark.stop()