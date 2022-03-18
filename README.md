# Project Description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.
The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
Here I have built an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.
The analytics team can then access the processed data to find insights into what songs their users are listening


### Datasets
Datasets used in this project resides in pulic access S3 bucket\
The datasets are JSON files namely song_data and log_data\
song_data holds the details of the song and its artists\
log_data hold the details of users which tell what songs is being heard and others\
These datasets will be fetched from S3, stage and processed in Redshift


### Database Schema
**Staging Tables**\
We have 2 staging tables - staging_events_table_create, staging_songs_table_create\
Data will be ingested into these table from the files that are located at S3

**Fact Table**\
*songplay* It has details of the even that is specific to page="NextSong"

**Dimension Tables**\
*users* - Details of the users using the app\
*songs* - Details of the song\
*artists* - Details of the artists of their respective songs\
*time*


### Data Warehouse Configurations and Setup
Create an IAM user into you aws account\
Create an S3 bucket that holds the json files song_data and log_data\
Create redshift cluster\
Create an IAM roles that allows redshift cluster to access S3 bucket\
Provide Host, Port, Access Key, ARN on the configuration files


### ETL
created and inserted records into staging tables from files present in S3 bucket\
created and inserted records into facts and dimension tables from staging tables


### How To Run the Project
**Step1:** Provide necessary aws connection details on dwh.cfg file\
**Step2:** Implement create_tables.py file which will drop and create tables using the sql_queries.py file\
**step3:** Run etl.py to do the ETL and load the data into staging tables anf then from staging tables load data into other tables

