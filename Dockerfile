FROM nedbank-de-challenge/base:1.0

# Fix Spark hostname resolution when running with --network=none
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV PARQUET_COMPRESSION=uncompressed

# Install any additional Python dependencies you need beyond the base image.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Delta jars directly from Maven into PySpark jars directory
RUN curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# Copy pipeline code and configuration into the image.
COPY pipeline/ pipeline/
COPY config/ config/

# Ensure config files are available at the expected runtime location
RUN mkdir -p /data/config
COPY config/pipeline_config.yaml /data/config/pipeline_config.yaml
COPY config/dq_rules.yaml /data/config/dq_rules.yaml

# Create output directory structure
RUN mkdir -p /data/output/bronze /data/output/silver /data/output/gold

# Set reasonable Spark memory limits (fits within 2GB container constraint)
ENV PYSPARK_SUBMIT_ARGS="--driver-memory 1g --executor-memory 1g pyspark-shell"

# Entry point — runs the complete pipeline end-to-end
CMD ["python", "-m", "pipeline.run_all"]