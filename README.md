![spark](Apache_Spark_logo.svg.png)


# Data Lake & Spark

## Introduction

Sparkify a music app want to create an ETL pipe line, since their data is growing, there is a need for data lake to smoothly process the data. Also plan to use AWS component for better data performace.

## Description 

As a Data Engineer for this project, main task is as follows 
- Create ETL pipeline
- Move data to s3 bucket
- Read data from S3 through Spark
- build end to end data flow using Spark, Hadoop (EMR cluster)
- Create schema on run using spark
- Write dimension and facts tables in form of parquet file

## Data sources
- songs data set <https://labrosa.ee.columbia.edu/millionsong/>
- logs data set: <https://github.com/Interana/eventsim>

## Project Files

- 'dwh.cfg': Config file for defining AWS attributes and local path for data.
- 'etl.py': Read all record from JSON files, create facts and dimension tables and write data in parquet files.
- 'etl.ipynb': testing the etl flow step by step.
- 'README.md': project info.
