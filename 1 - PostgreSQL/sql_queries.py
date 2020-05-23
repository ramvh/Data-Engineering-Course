# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL PRIMARY KEY
, start_time time not null
, user_id int REFERENCES users(user_id) ON DELETE CASCADE
, level varchar
, song_id varchar REFERENCES songs(song_id) ON DELETE CASCADE
, artist_id varchar REFERENCES artists(artist_id) ON DELETE CASCADE
, session_id varchar
, location varchar
, user_agent varchar);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY
, first_name varchar
, last_name varchar
, gender varchar
, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY
, title varchar
, artist_id varchar not null
, year numeric
, duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY
, name text
, location text
, latitude numeric
, longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time time PRIMARY KEY
, hour int
, day varchar
, week varchar
, month varchar
, year numeric
, weekday varchar);""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
songplay_id
, start_time
, user_id
, level
, song_id
, artist_id
, session_id
, location
, user_agent
) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users (
user_id
, first_name
, last_name
, gender
, level) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (
song_id
, title
, artist_id
, year
, duration) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists (
artist_id
, name
, location
, latitude
, longitude) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
""")


time_table_insert = ("""INSERT INTO time (
start_time
, hour
, day
, week
, month
, year
, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT s.song_id, s.artist_id 
FROM songs s
JOIN artists a ON s.artist_id = a.artist_id
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]