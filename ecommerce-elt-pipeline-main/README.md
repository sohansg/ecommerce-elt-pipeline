# 🛒 E-Commerce Data Pipeline

## 📄 Description
This project implements an ELT (Extract, Load, Transform) data pipeline for analyzing e-commerce transaction data from a UK-based online retailer. It uses a modern, containerized tech stack to enable efficient data ingestion, transformation, warehousing, and visualization. The pipeline facilitates actionable business insights through streamlined processes.

---

## 🚀 Features
- **Data Extraction**: Ingest raw e-commerce transaction data from CSV files.
- **Data Transformation**: Clean, aggregate, and enrich data using Apache Spark.
- **Data Warehousing**: Store processed data in Hive tables for querying.
- **Workflow Orchestration**: Manage pipeline tasks using Apache Airflow.
- **Scalability**: Leverage Hadoop for distributed data storage and processing.
- **Containerized Deployment**: Use Docker to run the entire stack seamlessly.

---
## 📝 Project Architecture
![Airflow Pipeline Graph](/output/ELT-Pipeline-Workflow.png)
*ELT pipeline workflow*

---

## 🛠️ Tech Stack

| Technology | Purpose | Version |
|--- |--- | --- |
| Hadoop | Distributed storage (HDFS) | 3.2.1 |
| Python | Programming and scripting  | 3.9 |
| Spark | Distributed data processing | 3.2.2
| Airflow | Workflow orchestration | 2.3.3 |
| Zeppelin | Web-based notebook for exploration | 0.10.1 |
| Hive | Data warehousing solution | 2.3.2 |
| Postgres | Hive metastore backend | 15.1 |

---

## 📂 Project Structure

```
.
├── dags/                        # Airflow DAGs for pipeline orchestration
├── configs/                     # Configuration files for Spark, Hive, and Hadoop
├── spark-scripts/               # Scripts for data ingestion and transformation
├── data/                        # Folder to store raw data
├── output/                      # Folder for results
├── streamlit/                   # Streamlit app for data visualization
└── docker-compose.yml           # Docker Compose file to orchestrate services

```

---

## 🚀 Quick Start Guide

### Prerequisites

- Docker
- Docker Compose
- Minimum 16GB RAM recommended
- Git

### Installation Steps

1. **Clone the Repository**
   ```bash
   git clone https://github.com/AbderrahmaneOd/spark-hive-airflow-ecommerce-pipeline.git
   cd spark-hive-airflow-ecommerce-pipeline
   ```

2. **Launch Infrastructure**
   ```bash
   docker-compose up -d
   ```

3. **Access Services**

   | Service | URL | Credentials |
   |---------|-----|-------------|
   | HDFS Namenode | http://localhost:9870 | - |
   | YARN ResourceManager | http://localhost:8088 | - |
   | Spark Master | http://localhost:8080 | - |
   | Spark Worker | http://localhost:8081 | - |
   | Zeppelin | http://localhost:8082 | - |
   | Airflow | http://localhost:3000 | admin@gmail.com / admin |

4. **Configure Spark Connection**
   - Navigate to Airflow UI
   - Go to Admin > Connections
   - Edit "spark_default"
     * Host: spark://namenode
     * Port: 7077

---

## 🔍 Pipeline Workflow

The Airflow DAG demonstrates a workflow:
1. Wait for data file
2. Upload data to HDFS
3. Load data using Spark
4. Transform data using Spark

---
   
## 📊 Data Visualization
The project includes a Streamlit app for intuitive visualization of business metrics, such as total revenue, top-selling products, and customer trends.

The Streamlit dashboard offers comprehensive e-commerce analytics:

- **Key Performance Metrics**:
  - Total Transactions
  - Total Quantity Sold
  - Total Revenue

- **Interactive Visualizations**:
  - Sales by Country
  - Top Selling Products
  - Sales Trend Over Time
  - Sales by Year

- **Filtering Capabilities**:
  - Country-based filtering
  - Date range selection
  - Downloadable filtered data
 
## 🖼️ Project Outputs

### Airflow Pipeline Visualization
![Airflow Pipeline Graph](/output/ecommerce_data_pipeline-Airflow.png)
*Airflow DAG (Directed Acyclic Graph) showing the pipeline workflow*

### Airflow Task Dependencies
![Airflow Task Dependencies](/output/ecommerce_data_pipeline-Graph-Airflow.png)
*Detailed view of task dependencies in the data pipeline*

### Streamlit E-Commerce Dashboard
![Streamlit Dashboard](/output/E-Commerce-Dashboard-Streamlit.png)
*Interactive dashboard for e-commerce sales analytics*
