# this file loads data from s3 into the staging tables then loads data from the staging tables into the fact/dimension tables.
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries 


def load_staging_tables(cur, conn):
    """This function loops through an array of staging table names
    and populates the tables with data from an S3 bucket using
    the COPY command.
    """
    for query in copy_table_queries:
        cur.execute(query)
        print('load table insert')
        conn.commit()


def insert_tables(cur, conn):
    """This function loops through an array of fact and dimension tables names
    and populates each table from the staging tables using INSERT statements.
    """
    for query in insert_table_queries:
        cur.execute(query)
        print('table insert')
        conn.commit()


def main():
    """This function establishes a connection to the Redshift cluster and database
    then calls the load_staging_tables method to load the retrieved data files from
    an S3 bucket into the two staging tables.  
    Next this function calls the insert_tables method to populate the song, artist,
    user and time tables with data from the staging tables.
    Lastly the songplays fact table is populated.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('We have a connection')
    
    print('Call function load_staging_tables')
    load_staging_tables(cur, conn)
    print('Call function insert_tables')
    insert_tables(cur, conn)
    
    print('loading complete')
    conn.close()

if __name__ == "__main__":
    main()
