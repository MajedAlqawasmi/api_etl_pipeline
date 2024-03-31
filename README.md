# Mixpanel API Pipeline (Iteration 1)
by [Majed Alqawasmi](https://github.com/MajedAlqawasmi) March 2024
<br/><br/>
Designing a data pipeline to ingest, process, and store incoming data from an external source Using an API endpoint from Mixpanel. The main focus is to explore various processes involved in automating a data pipeline. The pipeline handles data transformations, validation, and loading into a BigQuery database for further analysis


## Project Description 
- Develop a data processing module to handle API requests from an endpoint in Mixpanel, and extract any information relevant for business analysis
- Implement data validation and cleansing Python functions to validate and clean incoming data, by including checks for data completeness and consistency
- Ensure pipeline reliability by implementing retry mechanisms for failed API requests or database connections and ensuring error handling mechanisms are in place
- Implement data insertion or update logic, as well as a schedule mechanism to load data into BigQuery once a day


## Data
Three data frames can be found here [here](https://developer.mixpanel.com/reference/cohorts-list)


## Assumptions
- Mixpanel credentials are left empty due to the purpose of the exercise.
- Bigquery's table_id & dataset_id are left empty because of the purpose of the exercise.
- Extract, transform & load are in one task in the DAG to avoid using Xcom or intermediate storage since it's not relevant to the purpose of the challenge.
- "created" variable represent the time of cohort creation
- Apache Airflow is the platform used for pipeline orchestration