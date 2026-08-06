# 🚖 Ride Booking Lakehouse Pipeline

![Azure](https://img.shields.io/badge/Azure-Cloud-blue)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![PySpark](https://img.shields.io/badge/PySpark-Streaming-orange)
![Delta](https://img.shields.io/badge/Delta-Lake-green)
![SQL](https://img.shields.io/badge/SQL-Databricks-yellow)

An end-to-end streaming data engineering project built using Azure, Databricks, Delta Lake, and Lakeflow Declarative Pipelines.

## 🏗 Architecture

<p align="center">
    <img src="architecture/full.png" width="900">
</p>

## Bronze → Silver Processing

<p align="center">
    <img src="architecture/bronze_silver.png" width="900">
</p>

## 🛠 Tech Stack

| Category | Technology |
|-----------|------------|
| Cloud | Microsoft Azure |
| Data Factory | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks |
| Streaming | Lakeflow Declarative Pipelines |
| Language | PySpark |
| SQL | Databricks SQL |
| Storage Format | Delta Lake |
| Modeling | Star Schema |
| BI | Power BI |

```mermaid
flowchart LR

A[Legacy Data] --> B[Azure Data Factory]
API[Booking API] --> C[Streaming Ingestion]

B --> D[Bronze Layer]
C --> D

D --> E[Staging Streaming Table]

F[Mapping Tables] --> G[Jinja SQL Joins]

E --> G

G --> H[Silver OBT]

H --> I[Auto CDC]

I --> J[Gold Star Schema]
```
```text
RideBookingPipeline
│
├── bronze/
├── silver/
├── gold/
├── notebooks/
├── architecture/
│   ├── full.png
│   └── bronze_silver.png
├── README.md
```

## 📸 Lineage

<p align="center">
<img src="screenshots/lineage.png">
</p>


## ✨ Features

- Historical Batch Loading
- Real-Time Streaming Ingestion
- Medallion Architecture
- Delta Lake
- Lakeflow Declarative Pipelines
- Streaming Joins
- Watermarks
- Jinja SQL Templates
- Auto CDC
- SCD Type 1
- SCD Type 2
- Star Schema Modeling

