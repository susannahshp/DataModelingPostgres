import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Summary:
    This fucntion opens the song file with pandas dataframe and inserts the song record by executing song_table_insert query with song data list.
    It also inserts the artist record by executing artist_table_insert query with artist data list.
    
    Parameters:
    Cursor and filepath
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    select = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[select].values[0]
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    select =['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = df[select].values[0]
    artist_data = artist_data.tolist() 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Summary:
    This function opens the log file with pandas data frame and filters value and converts the column to date time value.
    Then it inserts time records by iterating through the index of the time data frame and executes time_table_insert query with row values.
    It also loads the user table and inserts user records by iterating through the index of the user data frame and executes the user_table_insert query and inserts the rows.
    For the songplay records, it iterates through the created log file data frame and gets the songid and artistid by executing song select query and inserts the songplay data by executing the songplay_table_insert query.
    
    Parameters:
    Cursor and filepath
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = (df['ts'],t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('start_time','hour','day','weekofyear','month','year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data))) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Summary:
    This function uses the os.walk with imported os module and walks through each directory to get all of the files matching json file format.
    Then it gets the total number of files found and iterates through the all files list and runs the parameter functions.
    It uses the string format function to show the files being processed to the user.
 
    Parameters:
    Cursor, connection, filepath, function.
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Summary:
    It connects to the Postgres database and runs the process data functions twice. One uses the song data file path and runs the process_song_file function inside and the other uses log data file path and runs the process_log_file function.
    Then it closes the connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()