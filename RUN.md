# How to Run the Pipeline

This guide provides the step-by-step commands to launch the entire IoT Data Pipeline.

**Prerequisite:** Ensure you have completed all installation and configuration steps in `SETUP.md`.

---

## 1. Pre-Run: Clean Previous State (IMPORTANT)

Before every new run (especially after a failure or code change), you **must** clear the old checkpoints and state data to avoid corruption.


# 1. Clear the Spark Checkpoint directory
```bash
rm -rf /home/mostafa/spark_project_data/checkpoints/farm_iot_full_pipelina
```

# 2. (Recommended) Clear the Data Lake and Parquet output
```bash
rm -rf /home/mostafa/spark_project_data/farm_iot_parquet/delta_lake/all_events
rm -rf /home/mostafa/spark_project_data/farm_iot_parquet/reliability_1h'''
```

## 2. Start the Pipeline
Open a new terminal window or tab for each step. The order is critical.

### Step 1: Start Backing Services
These services must be running before the Spark job.

**Terminal 1 - Zookeeper:**
```bash
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
```

**Terminal 2 - Kafka Broker:**
```bash
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```
**Terminal 3 - PostgreSQL:**
```bash
sudo systemctl start postgresql
```
**Terminal 4 - InfluxDB:**
```bash
sudo systemctl start influxdb
```
Wait a few seconds for all services to initialize.

### Step 2: Start Collection & Processing
**Terminal 5 - Telegraf:**
```bash

telegraf --config /path/to/your/telegraf.conf
```

**Terminal 6 - Spark Consumer (The Pipeline):**
```
Bash
spark-submit \
--packages io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
"/mnt/E/MyCareer/DepiData/DataHive/FinalProject/Consumer/Spark_Transformation_v1.0.py"
```

Wait until you see the "Streaming started..." message and Spark is processing Batch 0.

### Step 3: Start Ingestion & Visualization
**Terminal 7 - Data Producer:**
```Bash

python3 /mnt/E/MyCareer/DepiData/DataHive/FinalProject/Producer/IotSystem_Version1.1.py
```
**Terminal 8 - Grafana:**
```
Bash

sudo systemctl start grafana-server
```
Your pipeline is now fully operational. You can view the results at **http://localhost:3000**.

## 3. How to Stop the Pipeline
To stop the pipeline cleanly:

Press Ctrl+C in the Producer terminal (Terminal 7).

Press Ctrl+C in the Spark Consumer terminal (Terminal 6).

Press Ctrl+C in the Telegraf terminal (Terminal 5).

(Optional) Stop the backing services:
```
Bash

/opt/kafka/bin/kafka-server-stop.sh
/opt/kafka/bin/zookeeper-server-stop.sh
sudo systemctl stop postgresql
sudo systemctl stop influxdb
sudo systemctl stop grafana-server```
