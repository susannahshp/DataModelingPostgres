# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS FactSongPlays"
user_table_drop = "DROP TABLE IF EXISTS DimUsers"
song_table_drop = "DROP TABLE IF EXISTS DimSongs"
artist_table_drop = "DROP TABLE IF EXISTS DimArtists"
time_table_drop = "DROP TABLE IF EXISTS DimTime"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS FactSongPlays (
        songplay_id SERIAL PRIMARY KEY, 
        start_time BIGINT NOT NULL, 
        user_id VARCHAR NOT NULL, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS DimUsers (
        user_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS DimSongs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INT, 
        duration DECIMAL
);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS DimArtists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude DECIMAL, 
        longitude DECIMAL
);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS DimTime (
        start_time BIGINT PRIMARY KEY, 
        hour INT NOT NULL, 
        day INT NOT NULL, 
        week INT NOT NULL,
        month INT NOT NULL, 
        year INT NOT NULL, 
        weekday INT NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO FactSongPlays (songplay_id, start_time, user_id,
        level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""
    INSERT INTO DimUsers (user_id, first_name, last_name, gender, level)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")
   
song_table_insert = ("""
    INSERT INTO DimSongs (song_id, title, artist_id, year, duration)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO DimArtists (artist_id, name, location, latitude, longitude)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO DimTime (start_time, hour, day, week, month, year, weekday)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT DimSongs.song_id, DimArtists.artist_id 
    FROM DimSongs
    JOIN DimArtists
    ON DimSongs.artist_id = DimArtists.artist_id
    WHERE DimSongs.title = %s 
    AND DimArtists.name = %s 
    AND DimSongs.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]