# Data Warehousing with Redshift

## Due to tremendous growth, Sparkify, a music streaming service, wants to move its computing functions to the cloud.  

## The goal of this project is to extract raw data stored in an S3 Bucket on AWS and load them onto staging tables on Redshift.  Once on the staging tables, the data will be loading onto fact and dimension tables through SQL queries with some data transformation.

### Project Files
#### create_tables.py - This script executes the code to drop and create the staging and final analytic tables.
#### dwh.cfg - This file contains configuration variables and values.
#### etl.py - This script contains the logic to load the staging table, via a COPY command, and insert the staging table's data into the final tables.
#### sql_queries.py - This script contains all the drop table, create table, insert statements

### Implementation Steps
#### Log into your AWS account with this link:  https://console.aws.amazon.com/Redshift/
##### The IAM user represents an alias with the necessary permissions to acccess data on another AWS resource. 
##### If an IAM user does not exist for your account, use this link to create an IAM user: https://console.aws.amazon.com/iam/

#### Create an Amazon Redshift cluster via the launch cluster button.
##### Enter a value for the cluster identifier like redshift-cluster-1.
##### Enter a value for the database like dev.  The port should be set to 5439 for PostgreSql.
##### Enter the master username and password.
##### Select cluster-subnet-group-1 from the cluster subnet groups drop down.
##### Select the IAM role that has AmazonS3ReadOnlyAccess.
##### Verify the cluster configuration values and click launch cluster.  This will take a few minutes.  Once the cluster becomes "available", it is ready for use.

#### Open a terminal command line window and enter the following:  python3 create_tables.py
##### This will create all the necessary tables.

#### Next type: python3 etl.py
##### This will run the copy command to retrieve the data of the staging tables from an S3 bucket then populate the fact and dimension tables from the staging tabales.  Dimension tables contain data on songs, artists, users and time.  The fact table, songplays, is built from the dimension tables.  This can take up to 20 minutes to complete.

#### Once the tables are populated, go into the Query Editor on https://console.aws.amazon.com/Redshift/ and verify the data is correct in all tables.

### Project Tables
#### staging_songs_table - contains songs available for streaming
#### staging_events_table - contains user selections and playtimes
#### dimension tables include user_table, song_table, artist_table, time_table
#### fact table songplay_table

# Be sure to delete your Amazon Redshift cluster to avoid charges to your account.
