# this file loads data from s3 into the staging tables then loads data from the staging tables into the fact/dimension tables.
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries 


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        print('load table insert')
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        print('table insert')
        conn.commit()


def main():
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