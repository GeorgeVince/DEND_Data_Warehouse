import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging_songs and staging_events tables.

    Keyword arguments:
    cur -- cursor object to execute queries.
    conn -- connection to AWS cluster.
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into star schema.

    Keyword arguments:
    cur -- cursor object to execute queries
    conn -- connection to AWS cluster.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to cluster, load staging tables and perform ETL into star schema.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading staging tables...")
    load_staging_tables(cur, conn)
    print("Staging tables loaded")
    
    print("Inserting data from tables...")
    insert_tables(cur, conn)
    print("Finished loading")

    conn.close()


if __name__ == "__main__":
    main()