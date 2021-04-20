# This file contains drop/create tables statements and insert statements
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
# It is important to use the JSON key values for the column names in the create staging tables statement

staging_songs_table_create= (""" CREATE TABLE IF NOT EXISTS staging_songs_table
                                  ( song_id           VARCHAR,
                                    num_songs         INTEGER,
                                    title            VARCHAR,
                                    artist_name       VARCHAR,
                                    artist_latitude         FLOAT,
                                    year             INTEGER,
                                    duration         FLOAT,
                                    artist_id         VARCHAR,                              
                                    artist_longitude        FLOAT,
                                    artist_location         VARCHAR) 
""")

staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events_table
                                  ( artistName       VARCHAR,
                                    auth             VARCHAR,
                                    firstName        VARCHAR,
                                    gender           VARCHAR,
                                    itemInSession    INTEGER,
                                    lastName         VARCHAR,
                                    length           FLOAT,
                                    level            VARCHAR,
                                    location         VARCHAR, 
                                    method           VARCHAR,
                                    page             VARCHAR,
                                    registration     VARCHAR,
                                    sessionId        INTEGER,
                                    song             VARCHAR,
                                    status           VARCHAR,
                                    ts               BIGINT,
                                    userAgent        VARCHAR,
                                    userId           VARCHAR)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay_table
                            ( songplayId   BIGINT IDENTITY(0,1) PRIMARY KEY,
                              startTime    TIMESTAMP   not null,
                              userId       VARCHAR     not null,
                              level        VARCHAR     not null,
                              songId       VARCHAR     not null,
                              artistId     VARCHAR     not null,
                              sessionId    INTEGER     not null,
                              location     VARCHAR     not null,
                              userAgent    VARCHAR     not null)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS user_table
                            ( userId       VARCHAR     not null PRIMARY KEY SORTKEY,
                              firstName    VARCHAR     not null,
                              lastName     VARCHAR     not null,
                              gender       VARCHAR     not null,
                              level        VARCHAR     not null )
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS song_table
                            ( songId       VARCHAR     not null PRIMARY KEY SORTKEY,
                              title        VARCHAR     not null,
                              artistId     VARCHAR     not null,
                              year         INTEGER     not null,
                              duration     VARCHAR     not null )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist_table
                            ( artistId     VARCHAR     not null PRIMARY KEY SORTKEY,
                              artistName   VARCHAR     not null,
                              location     VARCHAR     not null,
                              latitude     FLOAT       null,
                              longitude    FLOAT       null )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time_table
                            ( startTime    TIMESTAMP   PRIMARY KEY SORTKEY,
                              hour         INTEGER     not null,
                              day          INTEGER     not null,
                              week         INTEGER     not null,
                              month        INTEGER     not null,
                              year         INTEGER     not null,
                              weekday      INTEGER     not null )
""")

# STAGING TABLES

staging_events_copy = (f"""
                      copy staging_events_table from {config.get('S3','LOG_DATA')}
                      credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
                      JSON {config.get('S3','LOG_JSONPATH')}
                      compupdate off region 'us-west-2';
                      """)

staging_songs_copy = (f"""
                     copy staging_songs_table from {config.get('S3','SONG_DATA')}
                     credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
                     JSON 'auto' 
                     compupdate off region 'us-west-2';
                     """)

# FINAL TABLES
                               
songplay_table_insert = (""" INSERT INTO songplay_table (startTime, userId, level, songId, artistId, sessionId, location, userAgent) 
                         SELECT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS startTime,
                                se.userId,
                                se.level,
                                ss.song_id,
                                ss.artist_id,
                                se.sessionid,
                                se.location,
                                se.userAgent
                           FROM staging_events_table se LEFT JOIN staging_songs_table ss 
                             ON se.artistName = ss.artist_name AND ss.title = se.song
                           WHERE se.page = 'NextSong' 
                             AND ss.song_id IS NOT NULL 
                             AND se.userId IS NOT NULL;""")

user_table_insert = (""" INSERT INTO user_table (userId, firstName, lastName, gender, level)
                     SELECT DISTINCT se.userId,
                            se.firstName,
                            se.lastName,
                            se.gender,
                            se.level
                       FROM staging_events_table se
                      WHERE se.page = 'NextSong' 
                        AND se.userId IS NOT NULL;""")

song_table_insert = (""" INSERT INTO song_table (songId, title, artistId, year, duration)
                     SELECT DISTINCT ss.song_id,
                            ss.title,
                            ss.artist_id,
                            ss.year,
                            ss.duration
                       FROM staging_songs_table ss
                      WHERE ss.song_id IS NOT NULL;""")

artist_table_insert = (""" INSERT INTO artist_table (artistId, artistName, location, latitude, longitude)
                       SELECT DISTINCT ss.artist_id,
                              ss.artist_name,
                              ss.artist_location, 
                              ss.artist_latitude,
                              ss.artist_longitude
                         FROM staging_songs_table ss
                        WHERE ss.artist_location  IS NOT NULL 
                          AND ss.artist_latitude  IS NOT NULL 
                          AND ss.artist_longitude IS NOT NULL;""")

time_table_insert = (""" INSERT INTO time_table (startTime, hour, day, week, month, year, weekday)
                     SELECT start_time, 
                            EXTRACT(hr from start_time)      AS hour,
                            EXTRACT(d from start_time)       AS day,
                            EXTRACT(w from start_time)       AS week,
                            EXTRACT(mon from start_time)     AS month,
                            EXTRACT(yr from start_time)      AS year, 
                            EXTRACT(weekday from start_time) AS weekday 
                       FROM (
                             SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time 
                             FROM staging_events_table s     
                             )
                      WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time_table);""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
