# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
userid numeric(8),
level int,
sessionId int,
location varchar(100),
userAgent varchar(100),
ts numeric(4,4),
artist_id int
)
""")

user_table_create = ("""
CREATE TABLE users(
userid numeric(8),
gender char(1),
first_name varchar(200),
last_name varchar(200),
level varchar(20)
song_id int,

)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id char(18),
artist_latitude float8,
artist_longitude float8,
artist_location varchar(100),
artist_name varchar(200)
)
""")


song_table_create = ("""
CREATE TABLE songs(
song_id char(18),
title varchar(100),
duration numeric(4,4),
num_songs int,
year int
)
""")

time_table_create = ("""
CREATE TABLE time
(
timestamp timestamp
week_of_year int,
weekday_name varchar(15),
year int, 
month int, 
day int,
hour int
)
""")

#INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
select artist_id from artists where artist_name = %s
union 
select song_id from songs where title = %s and duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]