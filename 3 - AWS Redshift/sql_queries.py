import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']
LOG_DATA = ['S3']['LOG_DATA']
LOG_JSONPATH = ['S3']['LOG_JSONPATH']
SONG_DATA = ['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events;"
staging_songs_table_drop = "drop table if exists stg_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stg_events (
                                artist TEXT
                                , auth TEXT
                                , firstName TEXT
                                , gender CHAR(1)
                                , itemInSession INTEGER
                                , lastName TEXT
                                , length NUMERIC
                                , level TEXT
                                , location TEXT
                                , method TEXT
                                , page TEXT
                                , registration numeric
                                , sessionId numeric
                                , song TEXT
                                , status INTEGER
                                , ts BIGINT
                                , userAgent TEXT
                                , userId INTEGER
                                )
                                """)

staging_songs_table_create =  ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                num_songs INTEGER,
                                artist_id TEXT,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id TEXT,
                                title TEXT,
                                duration NUMERIC,
                                year INTEGER)""")

songplay_table_create =  ("""CREATE TABLE IF NOT EXISTS songplay(
                            songplay_id INT IDENTITY(1,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id INTEGER NOT NULL,
                            level TEXT,
                            song_id TEXT,
                            artist_id TEXT,
                            session_id INTEGER,
                            location TEXT,
                            user_agent TEXT)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY,
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        gender CHAR(1),
                        level TEXT)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id TEXT PRIMARY KEY,
                        title TEXT,
                        artist_id TEXT,
                        year INTEGER,
                        duration NUMERIC )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
                          artist_id TEXT PRIMARY KEY,
                          name TEXT,
                          location TEXT,
                          latitude NUMERIC,
                          longitude NUMERIC )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekDay INTEGER  )""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events from {} iam_role {} json {}""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs from {} iam_role {} json 'auto'""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (
    SELECT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time, e.user_id, e.level, 
                                    s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
    FROM staging_events e
    JOIN staging_songs s
        ON e.artist = s.artist_name
        and e.length = s.duration
        and e.song = s.title
    WHERE e.page = 'NextSong'
)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (
    SELECT e.userId, e.firstName, e.lastName, e.gender, e.level
    FROM staging_events e
    WHERE e.page = 'NextSong'
)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (
    SELECT s.song_id, s.title, s.artist_id, s.year, s.duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL
    )
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES ( 
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE song_id IS NOT NULL
    )
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (
    SELECT start_time
        , extract(hour from start_time) as hour
        , extract(day from start_time) as day
        , extract(week from start_time) as week
        , extract(year from start_time) as year
        , extract(day from start_time) as day
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
