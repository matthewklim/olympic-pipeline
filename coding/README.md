# Coding Olympic Data Pipeline

The [olympic_data_ingestion.py](olympic_data_ingestion.py) script imports the raw JSON data from [athlete_events_2006_2016.jsonl.gz](./raw_data/athlete_events_2006_2016.jsonl.gz) into a SQL table in a locally hosted PostgreSQL database. The data is then extracted and transformed into the final three column output containing: year, season, and countries with medals.

The final `reporting.medal_summary` table results in the following 6 row output:

| year | season | countries_with_medals |
|------|--------|-----------------------|
| 2006 | Winter | 36                    |
| 2008 | Summer | 105                   |
| 2010 | Winter | 37                    |
| 2012 | Summer | 101                   |
| 2014 | Winter | 35                    |
| 2016 | Summer | 98                    |

## Pipeline Assumptions

* All files containing Olympic data will share the following:
  * delivered to the `raw_data` folder in this directory
  * start with `athlete_events_*` prefix followed by `start_year` and `end_year` in the name
    * e.g. athlete_events_<start_year>_<end_year>
* Files can be removed and deleted
  * Assumed there are processes to cleanup and archive old files. This could be manually maintained and initiated or automated
* For simplicity in this exercise, start year should not repeat while end_year can overlap. The python script references the start_year to look for the most recently created file, but could be modified to look for when the file was created or some other timestamp criteria
* For simplicity of this exercise assume, the python script assumes new data will always be new to insert new rows. In practice, the files may contain repeat data on athletes with updated information
* The python script will be scheduled as part of the overall data pipeline for ingesting source data and will be run through a platform like Airflow.
  * The chron job might run every hour during business days from Monday to Friday (chron expression: `0 * * * 1-5`)

### Other Assumptions

* Data Format: Assume that the Olympic data files are in JSONL format (JSON Lines), where each line represents a separate JSON object. This assumption aligns with the example file (athlete_events_2006_2016.jsonl.gz) used in the provided Python script.
* Data Schema Consistency: Assume that the structure and fields within the Olympic data files remain consistent across different files. This consistency allows for a uniform processing approach without the need for dynamic schema detection or handling different file structures.
* Data Delivery Mechanism: Assume that the Olympic data files are delivered to the raw_data folder on the local machine or a designated server accessible by the data pipeline. The specific mechanism of file delivery (e.g., manual upload, file transfer, API integration) is not within the scope of this exercise.
* Data Integrity and Validation: Assume that the Olympic data files undergo quality checks and validation before being processed. This can include verifying the file format, checking for data completeness, and performing data validation against defined rules or constraints.
* Pipeline Dependencies and Infrastructure: Assume that the data pipeline relies on an underlying infrastructure, such as a database server (e.g., PostgreSQL) and associated components (e.g., connectivity, storage, processing resources). These components are assumed to be in place and properly configured. Downstream SQL data pipelines like dbt are assumed to depend on ingestion sources to update by a certain time before running.

## Coding

Summary of the python script:

1. Read and parse the JSONL file containing Olympic data.
2. Connect to a PostgreSQL database using environment variables.
3. Import the parsed data into a raw table.
4. Create a processed data table with columns parsed from the json.
5. Populate the processed data table by selecting and converting relevant columns from the raw json table.
6. Close the database connection.

## 1. Database solution for Tableau Report

The python scripts creates three tables in the pipeline process:

1. `raw_data.olympics`: raw unfiltered json
2. `olympics.athlete_competitions`: all columns parsed into a SQL table
3. ` reporting.medal_summary`: summary table for Tableau

## 2. ETL Pipeline Components

In order to enhance the automation and efficiency of the existing pipeline, I would recommend the following changes and enhancements:

Implementing a data ingestion system: Instead of relying on manual delivery of data, we can set up a data ingestion system that automatically fetches new data from the source. This can be achieved through technologies like Apache Kafka or AWS Kinesis, which provide real-time data streaming capabilities.

Enhancing data validation and error handling: To ensure the quality of incoming data, we can introduce data validation checks at various stages of the pipeline. This includes validating data integrity, format, and schema compliance. Additionally, implementing robust error handling mechanisms and logging will help in identifying and resolving issues more effectively.

Scaling the data processing: As the volume of data increases, it's important to scale the data processing capabilities. We can explore options like distributed processing frameworks such as Apache Spark or cloud-based solutions like AWS EMR (Elastic MapReduce) to efficiently handle large-scale data transformations.

Utilizing containerization and orchestration: Containerization tools like Docker can be utilized to package the entire ETL pipeline, including dependencies and configurations. This ensures consistency across different environments and makes deployment easier. Coupled with container orchestration tools like Kubernetes, we can achieve better resource management and scalability.

Monitoring and alerting: Implementing a monitoring system will allow us to track the performance and health of the pipeline in real-time. We can leverage tools like Prometheus or ELK stack (Elasticsearch, Logstash, Kibana) to collect and visualize metrics. Setting up alerting mechanisms will notify the team about any issues or anomalies.

Continuous Integration and Deployment (CI/CD): To streamline the deployment of pipeline updates and ensure a smooth transition, we can adopt CI/CD practices. This involves setting up automated build, testing, and deployment pipelines using tools like Jenkins or GitLab CI.
