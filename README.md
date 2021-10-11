# Data Lake Project

## Introduction
The userbase and song database has for **Sparkify app** has grown in size, the data is stored in an AWS S3 Data lake in form of JSON Log files . This project focuses on moving the data back to S3 after extracting and transforming the data to fact and dimension tables from the orginal S3 data lake that has the userlog and songdata datasets . This project has an ETL Pipeline defined in the etl.py file that transforms the JSON log files using Spark and writes them back to the S3 in Apache parquet file format as it takes less storage space. The files stored as fact and dimension tables back in S3 which helps the Data analytics team to run their queries to analyse data.

We use star schema, of fact and dimension tables, that contains the songplay fact table and users, songs, artists and time table as dimensions table that allows analytics team to continue finding insights on what songs sparkify's users are listening to.  

ETL pipeline defined in the etl.py contains the functions that imports data from the S3 Data lake and reads it in a spark dataframe, the dataframes are then transformed into the fact and dimension tables in form of spark dataframes. Which are then written back to S3 as parquet files. Also we will see that when wirting these three tables below we use partitioning for parquet files:
- Songs table is partioned by year and artist id, 
- Time table is partitioned by year and month, 
- And the songplays fact table is partitioned by year and month. 

We do this so that the queries to analyse data from these tables can run quicker.

# How to Run project files?

## The Project contains two files:

- **dl.cfg** is a config file where we need to update the AWS access key id and Secret Access Key of the AWS User
- **etl.py** file which has the ETL Pipeline defined in it.

## Steps to run the ETL process:

- We need to create an EMR cluster in the same region as the S3 data lake. 
- Copy the etl.py and dl.cfg to the EMR cluster home folder.
- Update the aws access key id and Secret access key in dl.cfg
- Update the output_data variable in the main function with the s3 path where we want to write the parquet file.
- ssh to the EMR cluster and run **spark-submit etl.py** on the cluster.
- After the spark job is run succesfully we can then query the data from the fact and dimension table.