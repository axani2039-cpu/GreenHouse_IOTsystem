# GreenHouse_IOTsystem
# End-to-End IoT Data Pipeline Project

This project is a complete, end-to-end data pipeline that processes streaming data from IoT greenhouse sensors using Apache Spark. The data is cleaned, enriched, and a Star Schema model is built within a Data Warehouse, ready for analysis.

## 🏛️ Architecture

The pipeline consists of the following flow:

1.  **`Producer.py` (Producer):** Simulates IoT sensor data and sends it as JSON to a Kafka Topic (`farmSensors`).
2.  **`Telegraf` (Monitoring):** Reads the *same* topic (`farmSensors`) and sends the raw data to `InfluxDB` for real-time monitoring.
3.  **`Spark_Consumer.py` (Processor):**
    * Reads the stream from `farmSensors`.
    * **Performs Advanced Transformations:** (Cleaning, Enrichment, Stream-Static Join, Z-Score, ML Anomaly Detection, Sessionization).
    * **Writes to 4 Sinks:**
        * **Gold Layer (DWH):** Writes the final tables (`fact_sensor_events`, `daily_farm_kpis`, `farm_dry_sessions`) to `PostgreSQL`.
        * **Silver Layer (Data Lake):** Archives all processed events (`all_events`) to `Delta Lake`.
        * **Real-time Topics:** Pushes new insights back to `Kafka` (Topics: `farmInsights`, `farmTrends`, `farmKpis`).
        * **Monitoring Files:** Writes sensor reliability data (`reliability_1h`) as `Parquet` files.
4.  **`Grafana` (Visualization):** Connects to `PostgreSQL` (for analytics) and `InfluxDB` (for real-time monitoring) to display dashboards.



---

## 🛠️ Technology Stack

* **Data Ingestion:** Kafka
* **Data Processing:** Apache Spark (Structured Streaming)
* **Data Lake (Silver Layer):** Delta Lake
* **Data Warehouse (Gold Layer):** PostgreSQL
* **Real-time Monitoring:** Telegraf & InfluxDB
* **Data Visualization:** Grafana (or Power BI)
* **Language:** Python & SQL

---

## 📋 Requirements

### 1. Software

All of the following tools must be installed. (For detailed installation steps, see **[SETUP.md](SETUP.md)**).
* Java (JDK 17 recommended)
* Apache Spark (v4.0.0 or 4.0.1)
* Apache Kafka
* PostgreSQL
* InfluxDB
* Telegraf
* Grafana

### 2. Python Libraries (`requirements.txt`)

These libraries must be installed in the Python environment used by the Producer and the Spark Consumer.
(See **[requirements.txt](requirements.txt)**)

---

## ▶️ How to Run the Full Pipeline

To run the project, you must follow this order **strictly** to ensure dependencies are met.

### 1. (One-Time Setup) Initialize Project Directories

Ensure the directories for Spark's output exist:

```bash
mkdir -p /home/mostafa/spark_project_data/farm_iot_parquet/delta_lake/all_events
mkdir -p /home/mostafa/spark_project_data/farm_iot_parquet/reliability_1h
