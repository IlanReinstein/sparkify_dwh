import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
IAM_ROLE = config['IAM_ROLE']['ARN']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS stage_events (
	artist VARCHAR,
	auth VARCHAR,
	firstName VARCHAR,
	gender VARCHAR,
	itemInSession INT,
	lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    sessionId INT,
    song VARCHAR,
    status VARCHAR,
    ts BIGINT,
    userAgent VARCHAR,
    userId VARCHAR)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stage_songs (
	artist_id VARCHAR NOT NULL,
	artist_latitude NUMERIC,
	artist_longitude NUMERIC,
	artist_location VARCHAR,
	artist_name VARCHAR,
	song_id VARCHAR,
	title VARCHAR,
	duration NUMERIC,
	year NUMERIC)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
	songplay_id INT IDENTITY(0,1) PRIMARY KEY,
	start_time TIMESTAMP NOT NULL,
	user_id VARCHAR NOT NULL,
	level VARCHAR,
	song_id VARCHAR NOT NULL,
	artist_id VARCHAR NOT NULL,
	session_id BIGINT,
	location VARCHAR,
	user_agent VARCHAR
	)"""
                         )

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
	user_id VARCHAR PRIMARY KEY,
	first_name VARCHAR,
	last_name VARCHAR,
	gender VARCHAR,
	level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
	song_id VARCHAR PRIMARY KEY,
	title VARCHAR,
	artist_id VARCHAR NOT NULL,
	year NUMERIC,
	duration NUMERIC)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
	(artist_id VARCHAR PRIMARY KEY,
	name VARCHAR, location VARCHAR,
	latitude NUMERIC,
	longitude NUMERIC)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
	start_time TIMESTAMP PRIMARY KEY,
	hour NUMERIC,
	day NUMERIC,
	week NUMERIC,
	month NUMERIC,
	year NUMERIC,
	weekday NUMERIC)
""")

# STAGING TABLES
staging_events_copy = ("""COPY stage_events FROM {}
	credentials 'aws_iam_role={}'
	region 'us-west-2'
	format as json {};
	""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""COPY stage_songs FROM {}
	credentials 'aws_iam_role={}'
	region 'us-west-2'
	json 'auto'""").format(SONG_DATA, IAM_ROLE)

# SCHEMA TABLES

songplay_table_insert = ("""INSERT INTO songplays
	(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
	SELECT TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second' AS start_time,
	userid, level, songs.song_id, songs.artist_id, sessionid, location, useragent
	FROM stage_events events
	JOIN stage_songs songs
	ON songs.title = events.song
	AND songs.artist_name = events.artist
	AND songs.duration = events.length
	WHERE page = 'NextSong' ;
""")


user_table_insert = ("""INSERT INTO users
	(user_id, first_name, last_name, gender, level)
	SELECT userId, firstName, lastName, gender, level FROM stage_events
	WHERE page = 'NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
	SELECT song_id, title, artist_id, year, cast(duration as float) FROM stage_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
	SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS logitude
	FROM stage_songs;
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
	SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
		EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
		EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
		EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
		EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
		EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
		EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
		FROM stage_events
		WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
	songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
