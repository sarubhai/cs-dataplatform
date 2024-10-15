# Context
Designing a data ingestion system to handle data from various REST APIs in a big data environment. The system should efficiently ingest, store, and process the data for effective querying and reporting.

# High-Level System Design Overview

## Key Design Considerations

### Data Ingestion:
- Handle multiple REST APIs, each with different responses, authentication methods
- Integrate both batch and near-real-time data (call-back mechanism) ingestion APIs
- Support historical and daily new incremental data ingestion, ensuring efficient backfilling and updating of records

### Data Storage:
- Use scalable and cost-effective storage optimized for querying
- Maintain an audit trail of data changes for historical overview and reporting purposes

### Data Transformation and Processing:
- Harmonize data from various endpoints and structures
- Ensure the data is transformed and enriched with other data sources

### Monitoring and Data Integrity:
- Ensure that the ingestion pipeline can track failed or missed records
- Employ reconciliation strategies and alerts to handle inconsistencies (especially for real-time callbacks)

### Query and Reporting:
- Design for low-latency, high-throughput querying, and reporting with support for historical insights
- Leverage partitioning, clustering, and caching for cost-effective performance

### Data Governance:
- Data access is secured
- Data Products are well documented


## C4 Architecture Model

### 1. Context
![Alt text](context.png?raw=true "C4-Context")

### 2. Containers
![Alt text](containers.png?raw=true "C4-Containers")
- Data Ingestion Service: Responsible for fetching data from REST APIs, handling authentication, and transforming the data into a suitable format
- Data Storage: A big data storage system (e.g., Apache Hadoop, Amazon S3, Snowflake) to store the ingested data
- Data Processing: A data processing engine (e.g., Apache Spark, Snowflake) for data cleaning, transformation, and enrichment
- Data Warehouse: A cloud data warehouse (e.g., Amazon Redshift, Google BigQuery) for storing aggregated and historical data
- Data Exposition: A reporting tool (e.g., Tableau, Power BI) to visualize and analyze the data

### 3. Components
![Alt text](components.png?raw=true "C4-Components")

#### Data Ingestion
- Implement configurable template to handle different API endpoints, authentication methods, and response formats
- Airflow can be used as an Orchestrator for the batch data propessing pipeline
- Implement a retry strategy for failed API requests
- Pre process the ingested data into a consistent format suitable for storage and processing

#### Data Storgae
- AWS S3 is a big data storage system that meets the scalability, performance, and cost requirements
- Consider factors like data volume, query patterns, and integration with other tools
- S3 prefix based data partition will improve query performance and scalability
- Load the raw data into Snowflake based cloud data warehouse for efficient ELT integration & reporting

#### Data Processing
- ELT based approach leveraging DBT
- Data Cleaning: Remove duplicates, handle data quality issues like missing or invalid values, data inconsistencies and implement data & business validation rules
- Data Transformation: Transform the data into a format suitable for reporting or analysis
- Data Enrichment: Combine the data with other relevant datasets from the platform and create derived features or aggregate metrics
- Data Modification Tracking
- Implement a CDC mechanism to track changes in the data over time
- Implement SCD Type 2 to tracks and records data modifications for historical analysis



#### Data Exposition
- Reporting Tool Integration: Connect the reporting tool to the snowflake data warehouse
- Create dashboards and reports to visualize the data


#### Monitoring and Maintenance
- Implement data validation rules and anomaly detection
- Monitor data quality metrics to ensure data accuracy and completeness
- Implement monitoring tools to track system performance, resource usage, and error rates
- Set up alerts for critical issues
- Implement appropriate security measures to protect sensitive data




### 4. Code
![Alt text](code.png?raw=true "C4-Code")

- For Real-time data processing, supported REST API providers are configured with callback url set to API Gateway endpoint. The endpoint handler invokes Lambda Function to process the payload and save as json files to AWS S3 Bucket
- Consider using a message queue for decoupling and buffering callbacks
- Handle potential reliability issues with callbacks, via backfilling
- Snowpipe is used for real-time data ingestion from S3 to Snowflake
- Downstream data models can be designed as Dynamic tables to reflect the changes with source data
- Data Governence & Observability is achieved via Datahub

## Batch Data Ingestion Flow
![Alt text](ingestion.png?raw=true "Batch Ingestion")
