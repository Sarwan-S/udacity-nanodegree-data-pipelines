# Description
This application performs ETL on Airflow to create staging, fact and dimension tables on a Redshift cluster for optimising queries on song plays analysis for Sparkify. Sparkify would like to understand their users' usage patterns on their music streaming app.

Data collected by Sparkify include songs data files as well as event log data files, all of which are in JSON format and all of which sit on an Amazon S3 bucket. This application first loads these files into staging tables, from which it transforms data into a fact and dimension tables.

# Purpose of Database
This database would allow the Sparkify analytics team to perform queries to find the following type insights, among others:

1. Most active users based on 
2. Most popular songs based on numer of times the song is played
3. Most popular artists based on number of times their songs are played
4. Songs that specific users like listening to
5. Popular songs in specific geographies

The analytics team would be able to write flexible simplfied queries depending on the types of insights they would like to retrieve. This database also provides the following importances:

1. A standard data model is enforced.
2. Updates and insertions can be performed as more data is collected
3. Data integrity is maintained during insertions/modifications of the data
4. Data has an intuitive and simple organisation for flexible SQL queries

# Database Schema Design
A STAR schema design has been implemented. This simplifies the queries the analytics team will write through it's denormalised design. It also allows for faster aggregations as relevant information is captured in a denormalised structure.

This database consists of two staging tables, one fact table and five dimension tables. This design allows for the analytics team to run quick queries on the fact table to understand business process metrics on song plays by users in sessions. It also allows for queries to answer when, where and what type of questions (when were the songs played? which location were they played from? what songs are most popular?) using queries that combine the fact table with the dimension tables.

## Staging Tables
## staging_events
A staging table named **staging_events** captures a one to one mapping of all information from the events log files

The following attributes are captured in this staging table:

| Column          | Data Type |
|-----------------|-----------|
| event_id        | int       |
| artist          | varchar   |
| auth            | varchar   |
| first_name      | varchar   |
| gender          | char(1)   |
| item_in_session | int       |
| last_name       | varchar   |
| length          | numeric   |
| level           | varchar   |
| location        | varchar   |
| method          | varchar   |
| page            | varchar   |
| registration    | numeric   |
| session_id      | int       |
| song            | varchar   |
| status          | int       |
| ts              | bigint    |
| user_agent      | text      |
| user_id         | int       |

### staging_songs
A staging table named **staging_songs** captures a one to one mapping of all information from the songs data files

The following attributes are captured in this staging table:

| Column      | Data Type |
|-------------|-----------|
| num_songs   | int       |
| artist_id   | varchar   |
| latitude    | numeric   |
| longitude   | numeric   |
| location    | varchar   |
| artist_name | varchar   |
| song_id     | varchar   |
| title       | varchar   |
| duration    | numeric   |
| year        | int       |

## Fact Table
### songplays
A fact table named **songplays** captures information from the log data files about which songs were played by which user during a session.

The following attributes are captured in this fact table:

| Column      | Data Type |
|-------------|-----------|
| songplay_id | int       |
| start_time  | timestamp |
| user_id     | int       |
| level       | varchar   |
| song_id     | varchar   |
| artist_id   | varchar   |
| session_id  | int       |
| location    | varchar   |
| user_agent  | text      |

## Dimension Tables
### users
The **users** dimension table captures information from the log data files about the users of the music streaming app.

The following attributes are captured in this dimension table:

| Column     | Data Type |
|------------|-----------|
| user_id    | int       |
| first_name | varchar   |
| last_name  | varchar   |
| gender     | char(1)   |
| level      | varchar   |

If a user changes his level from free to paid or vice versa, the database only takes the latest record.

### songs
The **songs** dimension table captures information from the songs data files about songs in the music database.

The following attributes are captured in this dimension table:

| Column    | Data Type |
|-----------|-----------|
| song_id   | varchar   |
| title     | varchar   |
| artist_id | varchar   |
| year      | int       |
| duration  | numeric   |

### artists
The **artists** dimension table captures information from the songs data files about artists in the music database.

The following attributes are captured in this dimension table:

| Column    | Data Type |
|-----------|-----------|
| artist_id | varchar   |
| name      | varchar   |
| location  | varchar   |
| latitude  | numeric   |
| longitude | numeric   |

### time
The **time** dimension table captures information from the log data files about timestamps of records in **songplays** broken down into specific time units.

The following attributes are captured in this dimension table:

| Column     | Data Type |
|------------|-----------|
| start_time | timestamp |
| hour       | int       |
| day        | int       |
| week       | int       |
| month      | int       |
| year       | int       |
| weekday    | int       |

# ETL Pipeline

## Usage

1. Start your Airflow instance

2. Create a new connection variable (Admin > Connections) with the following:
  - Conn ID: aws_credentials
  - Conn Type: Amazon Web Services
  - Login: Access Key ID value from your IAM User credentials
  - Password: Secret Access Key value from your IAM User credentials

3. Create a new connection variable (Admin > Connections) with the following:
  - Conn ID: redshift
  - Conn Type: Postgres
  - Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
  - Schema: Enter dev. This is the Redshift database you want to connect to.
  - Login: Enter awsuser.
  - Password: Enter the password you created when launching your Redshift cluster.
  - Port: Enter 5439.

4. Switch on the DAG and run it manually
