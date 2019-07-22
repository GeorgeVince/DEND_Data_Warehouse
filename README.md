# Project 3 - Data Warehouse

The purpose of this project is to create a data warehouse to house song and log data from a music streaming site - called Sparkify.
The data resides in an S3 bucket, there are JSON logs on user acitivty and a directory of JSON metadata for songs contained on the app.

The project involves the following steps:

- An introduction into AWS, S3, Redshift and IAM Roles.
- Deploying clusters on AWS Redshift.
- Stage the data from S3 into AWS using the `copy` command.
- Perform ETL on the dataset to transform this from staging tables into a star schema.
- Perform some analysis on the dataset.

# Description of files

- `dwh.cfg` - AWS cluster configuration details, and the location of S3 bucket data.
- `sql_queries.py` - contains all of the required creation, insertion and deletion queries for the project.
- `create_tables.py` - connects to the cluster specified in `dwh.cfg`, creates the required tables using queries specified in `sql_queries.py`
- `etl.py` - the bulk of the heavy lifting happens here, data is staged into 2 staging tables then loaded into the appropriate fact and dimension tables.
- `explore_data.ipynb` - contains some example analysis queries.


# Schema Design
The data warehouse consists of two staging tables, a fact table and four dimension tables.  

## Songplays - Fact Table
The `songplays` table the fact table, it is sorted on start_time and distributed on song_id

```    
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id    INT     IDENTITY(0,1)  NOT NULL,
    start_time     FLOAT                  NOT NULL    sortkey, 
    user_id        INT                    NOT NULL, 
    level          VARCHAR                NOT NULL, 
    song_id        VARCHAR                NOT NULL    distkey, 
    artist_id      VARCHAR                NOT NULL, 
    session_id     INT                    NOT NULL, 
    location       VARCHAR                NOT NULL, 
    user_agent     VARCHAR                NOT NULL);
```
    
# Dimension Tables

## Users table
The `users` table is relatively small, so a distyle of ALL is chosen to copy to all slices.

```
    CREATE TABLE IF NOT EXISTS users (
    user_id   VARCHAR NOT NULL sortkey, 
    first_name VARCHAR, 
    last_name  VARCHAR, 
    gender     VARCHAR, 
    level      VARCHAR)
    diststyle all;
```

## Songs

The `songs` table is relatively large, so this is distributed over nodes using a dist key on song_id.

```
    CREATE TABLE IF NOT EXISTS songs (
    song_id      VARCHAR NOT NULL sortkey distkey, 
    title         VARCHAR, 
    artist_id     VARCHAR, 
    year          INT, 
    duration      FLOAT);"""
```

## Artists
The `artist` table is distributed across all nodes, since it is relatively small. E.g. One artist -> Multiple songs

```
    CREATE TABLE IF NOT EXISTS artists (
    artist_id    VARCHAR PRIMARY KEY NOT NULL sortkey,
    name          VARCHAR,
    location      VARCHAR,
    lattitude     FLOAT,
    longitude     FLOAT) 
    diststyle all;
```

## Time
```
    CREATE TABLE IF NOT EXISTS time (
    start_time       FLOAT PRIMARY KEY NOT NULL sortkey,
    hour             INT,
    day              INT,
    week             INT,
    month            INT,
    year             INT,
    weekday          INT)
    diststyle all;
```    

# Analytical Queries

See `explore_data.ipynb` for examples of analytical queries using the warehouse.