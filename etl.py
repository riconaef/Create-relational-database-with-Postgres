import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description:
        - Opens a file from the filepath and inserts the data from 
          this file into two different tables. 
        
    Arguments:
        cur:  the cursor object
        filepath: log data or song data file path
        
    Returns:
        None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', \
                      'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:
        - Opens a logfiles from the given filepath. 
        - The dataframe is filtered by "NextSong" in the column "page".
        - The "timestamp" column is transformed into the datetime format.
          The hour, day, week, month, year and weekday are separated out 
          into own columns and are filled in into the "time" table
        - The data of the users are filtered out and filled in into the 
          "users" table
        - The data of the "songplays" table is gathered together and written
          into the table. The log-files do not deliver the id for the songs 
          or artists. Therefore, a query is performed on the songname, artistname
          and the duration of the song, to find matches of the name. 
        
    Arguments:
        cur:  the cursor object
        filepath: log data or song data file path
        
    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts'].astype('datetime64[ms]')
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.week, t.dt.day, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('starttime', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_temp = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_temp[user_temp['firstName'].notnull()]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, \
                     row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)  


def process_data(cur, conn, filepath, func):
    """
    Description:
        This function opens all files in the given filepath and 
        iterates over all these files. During the iteration the
        given function "func" performs some processing steps on 
        each file. 
        
    Arguments:
        cur:  the cursor object
        conn: connection to the database
        filepath: log data or song data file path
        func: function that transforms the data and inserts it into the database
        
    Returns:
        None
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
    Description:
        Connects to the sparkifydb database and creates a cursor.
        Performs two functions, one for the song_data and one 
        for the log_data. The data of all these files are written 
        into 5 different tables. 
        
    Arguments:
        None
        
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()