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

* `raw_data.olympics`: This table stores the raw, unfiltered JSON data obtained from the Olympic Games source. It serves as the initial stage in the data pipeline, capturing the data in its original format.
* `olympics.athlete_competitions`: This table contains all the relevant columns parsed from the raw data and transformed into a structured SQL table. It serves as the primary data source for various analyses and visualizations in Tableau.
* `reporting.medal_summary`: This summary table aggregates and summarizes the medal-related information from the olympics.athlete_competitions table. It provides a condensed view of medal trends over time by sport and other key metrics, facilitating easier reporting and decision-making.

Once the pipeline is executed, the users will see their data reflected im their Tableau reports with data sources connected to either `olympics.athlete_competitions` or `reporting.medal_summary`. While not explored in this exercise, the data in `olympics.athelete_competitions` enables a number of analyses and visualizations for business stakeholders (e.g. medal trends over time by sport and any changes in weight and height of the athletes competing).

<details><summary>Five sample rows from the olympics.athlete_competitions of the highest medal earners</summary>
| team          | height | year  | sport      | noc | season | sex | event                                       | medal  | weight | city           | age | athlete_id | name                             |
| ------------- | ------ | ----- | ---------- | --- | ------ | --- | ------------------------------------------- | ------ | ------ | -------------- | --- | ---------- | -------------------------------- |
| Germany       | 165    | 2,016 | Gymnastics | GER | Summer | M   | Gymnastics Men's Horizontal Bar             | [NULL] | 55     | Rio de Janeiro | 28  | 85,712     | Marcel Van Minh Phuc Long Nguyen |
| Japan         | 160    | 2,016 | Gymnastics | JPN | Summer | M   | Gymnastics Men's Pommelled Horse            | [NULL] | 54     | Rio de Janeiro | 27  | 123,056    | Kohei Uchimura                   |
| Germany       | 164    | 2,016 | Gymnastics | GER | Summer | M   | Gymnastics Men's Team All-Around            | [NULL] | 62     | Rio de Janeiro | 28  | 45,219     | Fabian Hambchen                  |
| United States | 193    | 2,016 | Swimming   | USA | Summer | M   | Swimming Men's 4 x 100 metres Medley Relay  | Gold   | 91     | Rio de Janeiro | 31  | 94,406     | Michael Fred Phelps, II          |
| United States | 193    | 2,016 | Swimming   | USA | Summer | M   | Swimming Men's 200 metres Individual Medley | Gold   | 91     | Rio de Janeiro | 31  | 94,406     | Michael Fred Phelps, II          |

</details>

*Unsurprisingly, Michael Fred Phelps, II is the highest total medal winner in this dataset between 2006 and 2016 (He is also the highest medal winner of all time).*

## 2. ETL Pipeline Components

The python script created in part 1 is an incomplete data pipeline on its own and custom ingestion scripts have their own pros and cons.

### Benefits

* Customizable and flexible: The script can be tailored to specific requirements and adapt to changing needs.
* Cost-effective: Building an in-house solution eliminates the need for licensing costs associated with third-party tools.

### Challenges

* Difficulty with maintenance and changes: As the pipeline evolves, making updates and modifications to the script can be challenging and time-consuming.
* Lack of standardization: Without standardized practices, the pipeline may lack consistency in data processing logic, naming conventions, and overall structure.
* Responsibility for the entire pipeline: All aspects, from scheduling ingestions to transformations, error handling, and alerting, need to be handled within the script.
* Lack of change data capture implementation: Implementing change data capture functionality may be complex and is not handled in the Python script.
* Maintenance and scalability over time: As data volumes grow and requirements change, maintaining and scaling the script may become more difficult.

### Enhancements

The following approach and tools can help enhance the automation and efficiency of the existing pipeline. Implementing these enhancements can greatly enhance the data pipeline, but it's essential to balance these improvements with cost considerations. Evaluating the cost-effectiveness of each enhancement, including licensing fees, infrastructure costs, and ongoing maintenance expenses, will ensure that the pipeline remains sustainable and aligned with business cost constraints.

* Adopt pre-built tools: Consider incorporating tools like Fivetran, DataDog, or Monte Carlo for monitoring, alerting, data ingestion and validation. These tools offer out-of-the-box functionality, reducing the burden of maintenance and providing standardized practices.
* Introduce a dedicated transformation layer: Implement a tool like dbt (data build tool) as the primary transformation layer. This separates the transformation logic from the ingestion process and enables better maintainability and scalability.
* Flexible visualization and reporting: Utilize data tools like Looker or PowerBI for reporting purposes are excellent self service reporting tools. The data models in tools help make easy iterations, enhancing the visualization and reporting capabilities.
* Implement containerization and orchestration: Utilize containerization tools like Docker to package the pipeline and ensure consistency across different environments. Coupling it with orchestration tools like Kubernetes enables better resource management and scalability.
* Integrate change data capture: Evaluate the need for change data capture and explore options like Apache Kafka or AWS Kinesis to capture real-time data changes. This enables more granular updates to the pipeline and reduces the load on the system.
* Enhance monitoring and alerting: Implement a monitoring system using tools like Prometheus or ELK stack for real-time tracking of pipeline performance and health. Set up alerting mechanisms to promptly notify the team about any issues or anomalies.
