import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables from data warehouse.

    Keyword arguments:
    cur -- cursor object to execture queries
    conn -- connection to AWS cluster.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create required tables for data warehouse.

    Keyword arguments:
    cur -- cursor object to execture queries
    conn -- connection to AWS cluster.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to cluster, drop tables then create new tables.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to redshift cluster")
    print(*config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    print("Connected to cluster")
    cur = conn.cursor()


    print("Dropping tables")
    drop_tables(cur, conn)
    print("Tables dropped successfully...")
    
    print("Creating tables...")
    create_tables(cur, conn)
    print("Finishead creating tables...")
    
    print("Closing connection")
    conn.close()

    
if __name__ == "__main__":
    main()