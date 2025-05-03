from kafka import KafkaProducer
import json
import time
import random
import logging
import socket
import sys
from kafka.errors import NoBrokersAvailable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Function to create Kafka producer with retry logic
def create_producer(max_retries=10, retry_interval=5):
    retries = 0
    while retries < max_retries:
        try:
            logger.info("Attempting to connect to Kafka broker...")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=5,   # Retry a few times if sending fails
                retry_backoff_ms=1000,  # Wait 1s between retries
                request_timeout_ms=30000  # Allow 30s timeout for requests
            )
            logger.info("Successfully connected to Kafka broker!")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"No brokers available. Retry {retries+1}/{max_retries} in {retry_interval} seconds...")
            retries += 1
            time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            retries += 1
            time.sleep(retry_interval)
    
    logger.error(f"Failed to connect to Kafka after {max_retries} attempts.")
    return None

# Wait for Kafka to be available
logger.info("Waiting for Kafka to be available...")
time.sleep(30)  # Initial wait to ensure Kafka has time to start

# Create the producer with retry logic
producer = create_producer()

# If we couldn't create a producer after all retries, exit
if producer is None:
    logger.error("Could not connect to Kafka. Exiting.")
    sys.exit(1)

locations = ['Q1', 'Q3', 'Q5', 'Binh Thanh', 'Tan Binh']

# Start sending messages
logger.info("Starting to send messages to Kafka topic 'traffic'")
try:
    while True:
        message = {
            "location": random.choice(locations),
            "vehicle_count": random.randint(50, 500),
            "timestamp": time.time()
        }
        logger.info(f"Sending: {message}")
        producer.send('traffic', value=message)
        producer.flush()  # Ensure the message is sent
        time.sleep(2)
except KeyboardInterrupt:
    logger.info("Producer interrupted. Closing...")
except Exception as e:
    logger.error(f"Error in producer loop: {e}")
finally:
    if producer is not None:
        producer.close()
        logger.info("Producer closed.")
