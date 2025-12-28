# Crypto Whale Tracker: End-to-End ETL Pipeline

This project is a real-time Data Engineering pipeline designed to track high-value Bitcoin transactions (Whales) and store them for analytical purposes. The entire infrastructure is containerized using Docker and orchestrated by Apache Airflow.

## Architecture and Data Flow
The pipeline follows a modern data stack approach, ensuring data integrity and scalability:

* **Extraction**: A Python-based Collector service fetches live trade data via Binance/Crypto APIs and pushes it to the storage layer.
* **Raw Storage (Data Lake)**: The raw JSON data is archived in MinIO (S3-compatible object storage) to ensure a persistent source of truth.
* **Transformation (Orchestrated by Airflow)**: Data transformation logic is managed by Airflow tasks. This involves reading raw data from MinIO, applying cleaning procedures, and filtering movements based on a specific USD threshold.
* **Loading (Data Warehouse)**: The processed and filtered Whale data is loaded into a PostgreSQL database, structured for analytical queries and reporting.

## Tech Stack
* **Language**: Python
* **Orchestration**: Apache Airflow
* **Containerization**: Docker and Docker Compose
* **Object Storage**: MinIO
* **Relational Database**: PostgreSQL

## Project Structure
```text
.
├── airflow/
│   ├── dags/
│   │   └── whale_etl.py
│   └── plugins/
├── collector/
│   ├── Dockerfile
│   └── collector.py
├── infra/
│   └── postgres/
│       └── init.sql
├── docker-compose.yml
└── README.md
```

## Data Schema (PostgreSQL)
The filtered whale trades are stored with the following schema:

* **symbol**:String (e.g., BTCUSDT)
* **price**: Float (Execution price)
* **quantity**: Float (Trade volume)
* **value_usd**: Float (Total transaction value in USD)
* **side**: String (BUY or SELL)
* **trade_time**: Timestamp (UTC)

# How to Run
Ensure you have Docker and Docker Compose installed on your system:
```text
.
# Clone the repository
git clone [https://github.com/FatihEfeKavakli/etl-bitcoin-project.git](https://github.com/FatihEfeKavakli/etl-bitcoin-project.git)

# Navigate to the directory
cd etl-bitcoin-project

# Start all services
docker-compose up -d
```
Access the Airflow UI at http://localhost:8080 to monitor the DAGs.

## Default Credentials
Use the following credentials to access the services:
* **Airflow UI**: http://localhost:8080

  * User: admin
 
  * Pass: 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

* **MinIO Console**: http://localhost:9001

  * User: minioadmin

  * Pass: minioadmin

* **PostgreSQL**: localhost:5432

  * User: postgres

  * Pass: postgres
 

# Note on Threshold Logic
In a real-world production environment, "Whale" transactions are typically defined as $1M+ USD. To demonstrate the observability and functionality of this pipeline, the threshold has been intentionally set to **$100,000**. This allows for a more consistent data flow and more frequent validation of the ETL process during the demonstration and testing phases.

# Contact
Fatih Efe Kavakli

* **LinkedIn**: www.linkedin.com/in/fatihefekavaklı

* **GitHub**: @FatihEfeKavakli
