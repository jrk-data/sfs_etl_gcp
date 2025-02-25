# Data Ingestion and Analysis Project Documentation

## 1. Introduction
This project aims to extract, store, transform, and visualize data about a football team using cloud-based tools. The collected information is used to generate insights through dashboards in Looker Studio.

## 2. Architecture
The data flow in this project follows these steps:
1. **Scraping** a website using Python and ScraperFC.
2. **Storing raw data** in Google Cloud Storage.
3. **Loading data into BigQuery** for processing.
4. **Transforming data with Dataform** to generate optimized views.
5. **Consuming data in Looker Studio** for visualization.
6. **Automating the process with Cloud Functions and Cloud Scheduler** for periodic execution.

## 3. Technologies Used
- **Python** with ScraperFC for data extraction.
- **Google Cloud Functions** for automated execution of the scraping process.
- **Google Cloud Storage** for storing extracted data.
- **BigQuery** for data storage and modeling.
- **Dataform** for data transformation.
- **Looker Studio** for data visualization.
- **Cloud Scheduler** for automating the workflow execution.
- **GitHub** for source code management and version control.

## 4. Data Ingestion
The ingestion process is based on extracting information from a website using the ScraperFC library. The extracted data is stored in PARQUET format in Cloud Storage, enabling further processing in BigQuery.

## 5. Data Transformation

Data transformation is performed in two main stages: Python preprocessing and BigQuery/Dataform processing.

### Python Processing:

Multiple datasets are merged into a unified dataset.

A hashed field is generated based on other fields to detect changes in records, as the API does not provide information on the last update.

An ingestion date field is created to enable temporal tracking.

### BigQuery and Dataform Processing:

A log table is created in BigQuery, where new records or modified records are inserted, maintaining a history of changes.

A latest version table is derived from the log table, retaining only the most recent version of each record.

A final selection of relevant fields is performed, grouping data by football teams and applying aggregation calculations and new metric computations.

## 6. Visualization
The data sources created in BigQuery are connected to Looker Studio, where interactive dashboard is designed to explore the football team’s performance and identify relevant trends.

## 7. Automation
To ensure periodic data updates, the project implements:
- **Cloud Functions** to execute the scraping process and load data into Cloud Storage.
- **Cloud Scheduler** to trigger the Cloud Function at regular intervals.
- **IAM** to manage permissions and access to project resources.

## 8. Repository and Deployment
The project code is organized in a GitHub repository, including:
- Python scripts for scraping.
- Deployment configurations for the Cloud Function.
- Data transformation definitions in Dataform.
- Detailed documentation on setting up and executing the data workflow.

## 9. Conclusion and Future Improvements
This project provides an automated system for football team data ingestion and analysis. Future improvements could include:
- Expanding scraping to multiple data sources.
- Predictive analysis of team performance.
- Integration with additional visualization and advanced analytics tools.

---

This document serves as a guide to understanding the project’s structure and functionality, facilitating its replication and continuous improvement.

