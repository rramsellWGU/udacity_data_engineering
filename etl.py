import os
import glob
import psycopg2
import pandas as pd
import datetime as dt
from sql_queries import *
from sqlalchemy import create_engine


def process_song_file(cur, filepath):
    
#    The process_song_file function processes songfiles and inserts 
#    applicable artist and song data into their respective            
#    artists ans songs tables. The inputs for the function are 
#    cur: a db cursor, and filepath: a string filepath to the raw data.

    # open song file
    df = pd.read_json(filepath, lines=True)
    df.rename(columns={'artist_latitude':'latitude','artist_location':'location','artist_longitude':'longitude','artist_name':'name'}, inplace=True)
    # insert song record  
    for i, row in df.iterrows():
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(song_table_insert, song_data)
    # insert artist record
        artist_data = (row.artist_id, row.name, row.location, row.latitude, row.longitude)
        cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    
#    The process_log_file function processes logfiles and inserts 
#    applicable user, timestamp, and artist/song data into their respective            
#    songplays, time, and users tables. The inputs for the function are 
#    cur: a db cursor, and filepath: a string filepath to the raw data.

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['start_time'] = pd.to_datetime(df['ts'], format="%Y-%m-%d %H:%M:%S.%f")
    t = df.copy()
    
    # insert time data records
    t['ts'] =  pd.to_datetime(t['ts'], infer_datetime_format=True)
    t['start_time'] = pd.to_datetime(t['ts'], format="%Y-%m-%d %H:%M:%S.%f")
    t['hour'] = t['start_time'].dt.hour
    t['day'] = t['start_time'].dt.day
    t['week'] = t['start_time'].dt.week
    t['month'] = t['start_time'].dt.month
    t['year'] = t['start_time'].dt.year
    t['weekday'] = t['start_time'].dt.day_name()

    # Create time table
    time_df = t[['start_time','hour','day','week','month','year','weekday']]
        
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Prep df for user and songplay inserts
    df.rename(columns={'userId':'user_id','firstName':'first_name','lastName':'last_name','sessionId' : 'session_id','userAgent':'user_agent','song':'title','artist':'name','length':'duration'}, inplace=True)
    
    # load user table
    user_df = df[['user_id', 'first_name', 'last_name', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records    
    engine = create_engine("postgresql+psycopg2://student:student@127.0.0.1/sparkifydb")

    query = """
            select distinct s.song_id
                 , s.artist_id
                 , s.title
                 , a.name
                 , s.duration
            from songs s
            join artists a on a.artist_id = s.artist_id
            """

    df_q = pd.read_sql_query(query,con=engine)
    
    # Data types transform for join
    df_q['duration'] = df_q['duration'].astype('float64')
    df_q['title'] = df_q['title'].astype('str')
    df_q['name'] = df_q['name'].astype('str')
    
    df['duration'] = df['duration'].astype('float64')
    df['title'] = df['title'].astype('str')
    df['name'] = df['name'].astype('str')
    
    print(df_q.head(2))

    df_q = pd.merge(df,df_q,on=['title','name','duration'],how='left')
    df_q = df_q[['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']]
    
    print(df_q[df_q['song_id'].notnull()].head(5))
    
    for i, row in df_q.iterrows():
        cur.execute(songplay_table_insert, row)
        
def process_data(cur, conn, filepath, func):
    
#    The process_data function processes the json files for a directory filepath.
#    The inputs are cur: db cursor, conn: db connection script, filepath: string filepath, func: processing function.

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
    
#     This main funcion calls the other functions which process and insert raw data from the log and songs json files
#     into the database. Main method initiates the ingestion process with conn scripts, a db cursor, and the process_data             function.

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()