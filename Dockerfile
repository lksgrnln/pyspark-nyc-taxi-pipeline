FROM ubuntu:22.04
LABEL authors="lukas"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

ENV SPARK_SUBMIT_OPTS="-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000"

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# --- UPDATED TO HADOOP 3.3.6 TO NATIVELY SOLVE THE DURATION PARSING BUG ---
# 1. Download Hadoop AWS connector (3.3.6)
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    -O /usr/local/lib/python3.10/dist-packages/pyspark/jars/hadoop-aws-3.3.6.jar

# 2. Download AWS SDK Bundle (1.12.500 matches Hadoop 3.3.6 perfectly)
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.500/aws-java-sdk-bundle-1.12.500.jar \
    -O /usr/local/lib/python3.10/dist-packages/pyspark/jars/aws-java-sdk-bundle-1.12.500.jar

# 3. Download Wildfly OpenSSL
RUN wget -q https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar \
    -O /usr/local/lib/python3.10/dist-packages/pyspark/jars/wildfly-openssl-1.0.7.Final.jar
# --------------------------------------------------------------------------

CMD ["python3", "nyc_taxi_pipeline.py"]