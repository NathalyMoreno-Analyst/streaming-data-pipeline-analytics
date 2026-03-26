# Streaming Service Churn & Engagement Analytics 📺📊

## 📊 Dashboard Preview
![Análisis de Streaming](images/Imagen_Dashboard.png)

## 🎯 Business Context
In the highly competitive streaming industry, understanding user retention is critical. This project implements a comprehensive **End-to-End Data Pipeline** designed to ingest raw user interaction logs, process them through a modular ETL architecture, and deliver executive-level insights via an interactive dashboard.

The goal is to identify high-risk churn segments and optimize subscription profitability.

## 🏗️ Data Architecture & Pipeline
The project follows a professional **Modular Architecture**, separating concerns into three distinct layers:

1.  **ETL & Processing Layer (`scripts/data_processing.py`):** Handles data extraction and cleaning. It standardizes categorical variables and prepares dimensional tables.
2.  **Storage Layer (`scripts/database_manager.py`):** Deploys a relational schema using **SQLite**. It ensures data integrity by migrating cleaned CSVs into structured SQL tables.
3.  **Business Logic Layer (`scripts/business_logic.py`):** Executes advanced SQL queries and joins to generate specialized datasets for BI reporting, focusing on Churn, Engagement, and Revenue.

## 🛠️ Tech Stack
* **Language:** Python 3.12+ (Pandas, SQLAlchemy)
* **Database:** SQL (SQLite)
* **Business Intelligence:** Power BI
* **DevOps/Tooling:** Git, OS-Path Automation (for cross-platform compatibility)

## 🚀 Key Business Insights
* **Device Churn Correlation:** Identified that web-based users (Laptops) show a 15% higher churn rate than Smart TV users, suggesting UI/UX friction points.
* **Tier Profitability:** Premium users exhibit 2.5x more engagement time, validating the high-value segment strategy.
* **Retention Hotspots:** Specific regional patterns indicate that engagement drops significantly after the 3rd month of subscription.

## 📁 Project Structure
```text
streaming-data-pipeline-analytics/
├── data/               # Raw logs, SQL Database, and BI-ready CSVs
├── scripts/            # Modular Python ETL & Logic scripts
├── dashboard/          # Power BI (.pbix) source file
├── images/             # Visual documentation and previews
├── .gitignore          # Git exclusion rules
├── requirements.txt    # Environment dependencies
└── README.md           # Project documentation
