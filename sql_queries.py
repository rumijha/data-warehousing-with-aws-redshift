import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# FETCHING CONFIG DETAILS
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES IF IT ALREADY EXISTS

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATING TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(artist varchar, 
auth varchar, 
firstName varchar, 
gender varchar, 
iteminSession integer, 
lastName varchar, 
length numeric, 
level varchar,
location varchar, 
method varchar, 
page varchar, 
registration varchar, 
sessionid integer, 
song varchar, 
status integer, 
ts bigint, 
userAgent varchar, 
userid integer);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(num_songs integer, 
artist_id varchar, 
artist_latitude numeric, 
artist_longitude numeric, 
artist_location varchar, 
artist_name varchar, 
song_id varchar, 
title varchar, 
duration numeric, 
year integer);
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(user_id integer PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year integer, 
duration numeric);
""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
lattitude numeric, 
longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(start_time timestamp PRIMARY KEY, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday integer);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id varchar PRIMARY KEY, 
start_time timestamp not null, 
user_id integer not null, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id integer, 
location varchar, 
user_agent varchar);
""")


# INSERTING DATA INTO STAGING TABLES(IN REDSHIFT) FROM S3

staging_events_copy = ("""COPY staging_events 
FROM {}
credentials aws_iam_role={}
format json 'auto'
ACCEPTINVCHARS AS '^'
COMPUPDATE OFF STATUPDATE OFF
region 'us-east-2';
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""COPY staging_songs 
FROM {}
credentials 'aws_iam_role={}'
format json 'auto'
ACCEPTINVCHARS AS '^'
COMPUPDATE OFF STATUPDATE OFF
region 'us-east-2';
""").format(SONG_DATA, ARN)


# INSERTING DATA INTO TABLE FROM STAGING TABLES

songplay_table_insert = ("""INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, 
session_id, location, user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
* INTERVAL '1 second' as start_time,
se.userId as user_id,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId as session_id,
se.location,
se.userAgent as user_agent
FROM staging_events AS se
JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users
SELECT DISTINCT userId as user_id,
firstName as first_name,
lastName as last_name,
gender,
level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO songs
SELECT DISTINCT song_id,
title,
artist_id,
year,
duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists
SELECT DISTINCT artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 \
* INTERVAL '1 second' as start_time,
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week,
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year,
EXTRACT(week FROM start_time) as weekday
FROM    staging_events
WHERE page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
