# Detailed Setup Guide

This guide explains how to install every tool required to run the pipeline from scratch.

## 1. 🐍 Python 3.10+

Ensure you have Python 3.10 or newer installed.

### Install Libraries (Requirements)

1.  (Optional but recommended) Create a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
2.  Install all required libraries:
    ```bash
    pip install -r requirements.txt
    ```

---

## 2. 🗄️ PostgreSQL (Data Warehouse)

1.  **Install PostgreSQL:**
    ```bash
    sudo apt update
    sudo apt install postgresql postgresql-contrib
    ```
2.  **Start and enable the service:**
    ```bash
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
    ```
3.  **Create the User and Database:**
    ```bash
    # 1. Log in to psql as the superuser
    sudo -u postgres psql

    # 2. Create the user for Spark (replace 'spark_password' with a strong password)
    CREATE USER spark_user WITH PASSWORD 'spark_password';

    # 3. Create the database
    CREATE DATABASE farm_dwh OWNER spark_user;

    # 4. Grant privileges (Very important)
    GRANT ALL PRIVILEGES ON DATABASE farm_dwh TO spark_user;

    # 5. Exit
    \q
    ```

---

## 3. ✒️ Apache Kafka

1.  **Install Java 17:** (Kafka requires Java)
    ```bash
    sudo apt update
    sudo apt install openjdk-17-jdk
    ```
2.  **Download Kafka:**
    * Go to the [Kafka downloads page](https://kafka.apache.org/downloads) and copy the link for the latest binary (e.g., 3.x.x).
    * ```bash
        wget [Kafka_Download_Link]
        tar -xzf kafka_2.13-3.x.x.tgz
        mv kafka_2.13-3.x.x /opt/kafka
        ```
3.  **Create Topics:** (Run this before starting the pipeline)
    * (Make sure Zookeeper and Kafka are running before executing)
    * **Raw Data Topic:**
        ```bash
        /opt/kafka/bin/kafka-topics.sh --create --topic farmSensors --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        ```
    * **Spark Output Topics (Optional):**
        ```bash
        /opt/kafka/bin/kafka-topics.sh --create --topic farmInsights --bootstrap-server localhost:9092
        /opt/kafka/bin/kafka-topics.sh --create --topic farmTrends --bootstrap-server localhost:9092
        /opt/kafka/bin/kafka-topics.sh --create --topic farmKpis --bootstrap-server localhost:9092
        ```
4.  **Run Commands (Reference):**
    * **Zookeeper:** `/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties`
    * **Kafka:** `/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties`

---

## 4. ✨ Apache Spark

(Assuming you are running Spark 4.0.1 as seen in logs)

1.  **Download Spark:**
    * Go to the [Spark downloads page](https://spark.apache.org/downloads.html).
    * Select `4.0.1` and download the pre-built binary.
    * ```bash
        wget [Spark_Download_Link]
        tar -xzf spark-4.0.1-bin-hadoop3.tgz
        sudo mv spark-4.0.1-bin-hadoop3 /opt/spark
        ```
2.  **Set `SPARK_HOME` (Recommended):**
    * Add this line to your `~/.bashrc` file:
        `export SPARK_HOME=/opt/spark`
    * Refresh your terminal: `source ~/.bashrc`

---

## 5. ⏳ InfluxDB & Telegraf

### InfluxDB (Monitoring Database)

1.  Follow the [Official Install Guide](https://docs.influxdata.com/influxdb/v2/install/) for your OS.
2.  **Start and enable the service:**
    ```bash
    sudo systemctl start influxdb
    sudo systemctl enable influxdb
    ```
3.  **Initial Setup:**
    * Go to `http://localhost:8086`.
    * Create a user and password.
    * **Very Important:** Create an `Organization` (e.g., `my_org`) and a `Bucket` (e.g., `iot_bucket`).
    * Go to the `API Tokens` section and generate a new `All-Access Token`. **Save this token.**

### Telegraf (Data Collector)

1.  Follow the [Official Install Guide](https://docs.influxdata.com/telegraf/v1/install/) for your OS.
2.  **Create Configuration File:**
    * Create a file named `telegraf.conf`.
    * Copy the content below, **and update the InfluxDB values**:

    ```toml
    [agent]
      interval = "5s"
      flush_interval = "5s"

    # 1. Input: Read from Kafka
    [[inputs.kafka_consumer]]
      brokers = ["localhost:9092"]
      topics = ["farmSensors"]
      consumer_group = "telegraf_monitor_group"
      offset = "oldest"
      data_format = "json"

    # 2. Output: Write to InfluxDB
    [[outputs.influxdb_v2]]
      urls = ["http://localhost:8086"]
      token = "YOUR_INFLUXDB_TOKEN_HERE"    # <--- REPLACE
      organization = "YOUR_ORG_NAME_HERE"   # <--- REPLACE
      bucket = "YOUR_BUCKET_NAME_HERE"      # <--- REPLACE
    ```
3.  **Run Command (Reference):**
    * `telegraf --config /your/path/to/telegraf.conf`

---

## 6. 📊 Grafana (Dashboard)

1.  Follow the [Official Install Guide](https://grafana.com/docs/grafana/latest/installation/) for your OS.
2.  **Start and enable the service:**
    ```bash
    sudo systemctl start grafana-server
    sudo systemctl enable grafana-server
    ```
3.  **Login:**
    * Go to `http://localhost:3000`.
    * (Default user/pass: `admin`/`admin`).
4.  Add `PostgreSQL` and `InfluxDB` as Data Sources as shown in the main `README.md`.
