import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from sqlalchemy import create_engine


def process_song_file(cur, engine, filepath):
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    df_song = pd.DataFrame(df,columns=["song_id", "title", "duration", "num_songs", "year"])
    df_song.to_sql("songs", con=engine, if_exists='append', index = False)
   
    # insert artist record
    df_artist_data = pd.DataFrame(df,columns=["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"])
    df_artist_data.to_sql("artists", con=engine, if_exists='append', index = False)    

    
def process_log_file(cur,engine ,filepath):
    # open log file
    
    df = pd.read_json(filepath,lines=True)
    df = df[df.page =='NextSong']
    
    df_time = pd.DataFrame(pd.to_datetime(df['ts'], unit='ms'))

    df_time = df_time.rename(columns={"ts":"timestamp"})

    df_time["week_of_year"] = df_time.timestamp.dt.week
    df_time["weekday_name"] = df_time.timestamp.dt.weekday_name
    df_time["year"] = df_time.timestamp.dt.year
    df_time["month"] = df_time.timestamp.dt.month
    df_time["day"] = df_time.timestamp.dt.day
    df_time["hour"] = df_time.timestamp.dt.hour

    df_time.head()
    df_time.to_sql("time", con=engine, if_exists='append', index = False)    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows(): 
        # get songid and artistid from song and artist tables
        cur.execute(song_select,(row.artist,row.song,row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        df["song_id"] = songid
        df["artist_id"] = artistid

                
    song_plays = pd.DataFrame(df,columns=["userId","level","sessionId","location","userAgent","ts","song_id","artist_id"])
    song_plays.to_sql("songplays", con=engine, if_exists='append', index = False)



def process_data(cur, engine, filepath, func):
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
        func(cur,engine, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    engine = create_engine('postgresql://student:student@localhost/sparkifydb')
    cur = conn.cursor()

    process_data(cur, engine, filepath='data/song_data', func=process_song_file)
    process_data(cur, engine, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()