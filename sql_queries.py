# Drop statement
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# Create statements

songplay_table_create = ("""
create table if not exists songplays
(
  songplay_id serial primary key
, start_time timestamp not null
, user_id bigint not null
, level varchar
, song_id varchar
, artist_id varchar 
, session_id varchar
, location varchar
, user_agent varchar
)
""")

user_table_create = ("""
create table if not exists users
(
  user_id bigint primary key
, first_name varchar
, last_name varchar
, gender varchar
, level varchar
)
""")

song_table_create = ("""
create table if not exists songs
(
  song_id varchar primary key
, title varchar not null
, artist_id varchar not null
, year int
, duration float
)
""")

artist_table_create = ("""
create table if not exists artists
(
  artist_id varchar primary key
, name varchar not null
, location varchar
, latitude double precision
, longitude double precision
)
""")

time_table_create = ("""
create table if not exists time 
(
  start_time timestamp primary key
, hour int
, day int
, week int
, month int
, year int
, weekday varchar
)
""")

# INSERT RECORDS

songplay_table_insert = """
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing
"""

user_table_insert = """
insert into users (user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s) on conflict (user_id) do update set level=EXCLUDED.level
"""

song_table_insert ="""
insert into songs (song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s) on conflict do nothing
"""

artist_table_insert ="""
insert into artists (artist_id, name, location, latitude, longitude) values (%s, %s, %s, %s, %s) on conflict do nothing
"""


time_table_insert ="""
insert into time (start_time, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s) on conflict do nothing
"""

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
