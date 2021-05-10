import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables from JSON into Redshift.
    -------------------
    Param:
        cur: SQL cursor to execute queries
        conn: Connection instance to commit changes
    Return:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from stage into DW.
    -------------------
    Param:
        cur: SQL cursor to execute queries
        conn: Connection instance to commit changes
    Return:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()
    print("loading stage tables")
    load_staging_tables(cur, conn)
    print("loading schema tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
