import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Summary:
    This is a function to create a database.
    It creates a connection object with psycopg2 module with hostname, user and password information. 
    Then it sets the autocommit to True so you won't have to commit everytime.
    First it connects to an existing database and then it creates a new dastabase named sparkifydb.

    Returns: 
    Cursor and connection objects.
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Summary:
    This function grabs a query from drop_table_queries list and executes the query with the cursor.
    
    Parameters:
    Cursor and connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Summary:
    This function iterates through the create_table_queries list and executes query with the cursor.
    
    Parameters:
    Cursor and connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Summary:
    This function runs the create_databse function and sets cursor and connection to the return values.
    It also runs the drop_tables and create_tables function.
    Then it closes the connection.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()