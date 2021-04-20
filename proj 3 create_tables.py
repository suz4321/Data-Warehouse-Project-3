import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function will drop all tables and runs prior to the
    create tables function.  Table to be dropped are as follows:
    staging_songs_table
    staging_events_table
    song_table
    artist_table
    user_table
    time_table
    songplays_table
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function will create all tables necessary to run the ETL script.
    Table to be created are as follows:
    staging_songs_table
    staging_events_table
    song_table
    artist_table
    user_table
    time_table
    songplays_table
    Both staging tables should not be created with identity keys or be assigned NULL/NOT NULL attributes
    as these tables contain raw data.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function establishes a connection to the Redshift cluster and database
    then calls the drop_table method and create_tables method.  The newly created
    empty tables are populated by the etl.py script.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print('Drop tables complete')
    create_tables(cur, conn)
    print('Create tables complete')
    conn.close()

if __name__ == "__main__":
    main()
