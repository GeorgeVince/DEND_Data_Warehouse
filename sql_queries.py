import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist         VARCHAR,
                                                                            auth           VARCHAR,
                                                                            firstName      VARCHAR,
                                                                            gender         VARCHAR,
                                                                            itemInSession  SMALLINT,
                                                                            lastName       VARCHAR,
                                                                            length         FLOAT,
                                                                            level          VARCHAR,
                                                                            location       VARCHAR,
                                                                            method         VARCHAR,
                                                                            page           VARCHAR,
                                                                            registration   FLOAT,
                                                                            sessionId      SMALLINT,
                                                                            song           VARCHAR,
                                                                            status         INT,
                                                                            TS             FLOAT,
                                                                            userAgent      VARCHAR,
                                                                            userId         INT);""")


staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs        INT,
                                                                             artist_id        VARCHAR,
                                                                             artist_latitude  FLOAT,
                                                                             artist_longitude FLOAT,
                                                                             artist_location  VARCHAR,
                                                                             artist_name      VARCHAR,
                                                                             song_id          VARCHAR,
                                                                             title            VARCHAR,
                                                                             duration         FLOAT,
                                                                             year             INT);""")

##songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id   INT     IDENTITY(0,1)  NOT NULL,
                                                                 start_time     FLOAT                  NOT NULL    sortkey, 
                                                                 user_id        INT                    NOT NULL, 
                                                                 level          VARCHAR, 
                                                                 song_id        VARCHAR                NOT NULL    distkey, 
                                                                 artist_id      VARCHAR                NOT NULL, 
                                                                 session_id     INT, 
                                                                 location       VARCHAR, 
                                                                 user_agent     VARCHAR                );""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id   VARCHAR NOT NULL sortkey, 
                                                         first_name VARCHAR, 
                                                         last_name  VARCHAR, 
                                                         gender     VARCHAR, 
                                                         level      VARCHAR)
                                                         diststyle all;""")

#song_id, title, artist_id, year, duration
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id      VARCHAR NOT NULL sortkey distkey, 
                                                         title         VARCHAR, 
                                                         artist_id     VARCHAR NOT NULL, 
                                                         year          INT, 
                                                         duration      FLOAT);""")

#artist_id, name, location, lattitude, longitude
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id    VARCHAR PRIMARY KEY NOT NULL sortkey,
                                                             name          VARCHAR,
                                                             location      VARCHAR,
                                                             lattitude     FLOAT,
                                                             longitude     FLOAT) 
                                                             diststyle all;""")

#start_time, hour, day, week, month, year, weekday
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time       FLOAT PRIMARY KEY NOT NULL sortkey,
                                                         hour             INT,
                                                         day              INT,
                                                         week             INT,
                                                         month            INT,
                                                         year             INT,
                                                         weekday          INT)
                                                         diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    JSON {} truncatecolumns;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_songs 
    from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
##songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT
                                stg_events.ts as start_time,
                                stg_events.userid as user_id,
                                stg_events.level as level,
                                stg_songs.song_id as song_id,
                                stg_songs.artist_id as artist_id,
                                stg_events.sessionid as session_id,
                                stg_events.location as location,
                                stg_events.useragent as user_agent
                            FROM
                                staging_events as stg_events
                            JOIN
                                staging_songs as stg_songs
                            ON
                                (stg_events.artist = stg_songs.artist_name
                                 AND
                                 stg_events.song = stg_songs.title
                                 AND 
                                 stg_events.length = stg_songs.duration)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT userId as user_id, 
                               firstName as first_name, 
                               lastName as last_name, 
                               gender, 
                               level
                        FROM staging_events
                        where userId is NOT NULL""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration
                        FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          SELECT DISTINCT artist_id, 
                                          artist_name as name, 
                                          artist_location as location, 
                                          artist_latitude as latitude, 
                                          artist_longitude as longitude
                          FROM staging_songs""")

#https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT
                              start_time,
                              EXTRACT(hour FROM t.TIMECOL) as hour,
                              EXTRACT(day FROM t.TIMECOL) as day,
                              EXTRACT(week FROM t.TIMECOL) as week,
                              EXTRACT(month FROM t.TIMECOL) as month,
                              EXTRACT(year FROM t.TIMECOL) as year,
                              EXTRACT(weekday FROM t.TIMECOL) as weekday
                        FROM (
                              SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as TIMECOL,
                              ts as start_time
                              FROM staging_events
                              ) as t""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

