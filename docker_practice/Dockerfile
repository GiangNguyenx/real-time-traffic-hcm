FROM bitnami/spark:3.5.5

WORKDIR /app

# Create directories for checkpoints and jars
RUN mkdir -p /app/checkpoints/traffic
RUN mkdir -p /app/jars

# Copy application
COPY spark_streaming_app.py /app/

# We'll use the default Spark entrypoint and let docker-compose handle the environment variables
CMD ["bash", "-c", "sleep 10 && /opt/bitnami/spark/bin/spark-submit --verbose --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:3.5.1 --conf spark.executor.extraClassPath=/app/jars/*:/opt/bitnami/spark/jars/* --conf spark.driver.extraClassPath=/app/jars/*:/opt/bitnami/spark/jars/* /app/spark_streaming_app.py"]