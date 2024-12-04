# Project: Data Pipeline with Airflow

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Project Description

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step. 
We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.
You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## Datasets
The datasets used in this project are stored within S3 buckets, which can be found under the following URIs:

Log data: `s3://udacity-dend/log_data`
Song data: `s3://udacity-dend/song_data`

## File descriptions

The project has a main directory, `airflow`, which contains two further directories named `dags` and `plugins`. 

`dags` directory contains:
- `udac_example_dag.py`: Defines main DAG, tasks and link the tasks in required order.
- `create_tables.sql`: SQL create table statements provided with template.

`plugins/operators` directory contains:
- `stage_redshift.py`: Defines `StageToRedshiftOperator` to copy JSON data from S3 to staging tables in the Redshift via `copy` command.
- `data_quality.py`: Defines `DataQualityOperator` to run data quality checks on all tables passed as parameter
- `load_dimension.py`: Defines `LoadDimensionOperator` to load a dimension table from staging table(s)
- `load_fact.py`: Defines `LoadFactOperator` to load fact table from staging table(s)
- `sql_queries.py`: Contains SQL queries for the ETL pipeline

## Configurations

- Create a Redshift cluster in `us-west2` 
- Within the Udacity workspace terminal, run the following command: `/opt/airflow/start.sh` and subsequently connect with the Airflow UI
- Within Airflow, add the following two connections:
    - AWS credentials, named `aws_credentials`
    - Connection to Redshift, named `redshift`
- The DAG named `udac_example_dag` should be visible in the UI - click on run

## Disclaimer

Data and project information were kindly provided by [Udacity](https://www.udacity.com/).