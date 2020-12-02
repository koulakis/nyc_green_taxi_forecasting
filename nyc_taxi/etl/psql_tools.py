import io

import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class TransactionManager:
    def __init__(
            self,
            database='nyc_green_taxi',
            user='mariosk',
            password='pass',
            host='localhost',
            port=5432):
        """Wrapper around psycopg2 and sqlalchemy which provides simple functions to handle psql transactions.
        NOTE: This class allows pretty unsafe transactions (e.g. pass a query as string) which could lead to sql
        injection. Use with caution!

        Args:
            database: database name
            user: user name
            password: user password
            host: the hostname of the database server
            port: the database server port
        """
        self.conn_dict = dict(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port)

        self.conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(self.conn_string)

    def pd_read_psql(self, query):
        """Query data from the database to a pandas dataframe.

        Args:
            query: a sql query

        Returns:
            A dataframe containing the output table of the query.
        """
        if query.endswith(';'):
            query = query[:-1]
        copy_sql = f'COPY ({query}) TO STDOUT WITH CSV HEADER;'
        with psycopg2.connect(**self.conn_dict) as conn:
            try:
                with conn.cursor() as cursor:
                    with io.StringIO() as cache:
                        cursor.copy_expert(copy_sql, cache)
                        cache.seek(0)
                        return pd.read_csv(cache)
            except Exception as e:
                print(e)
                conn.rollback()

    def create_index_on_column(self, column, table_name):
        """Create an index on a given column.

        Args:
            column: the name of the column
            table_name: the name of the table
        """
        with psycopg2.connect(**self.conn_dict) as conn:
            with conn.cursor() as cursor:
                self.execute_command(cursor, f'CREATE INDEX {column}_idx_{table_name} ON {table_name} (column);', conn)

    @staticmethod
    def execute_command(cursor, command, conn):
        try:
            cursor.execute(command)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()

    def create_table_from_csv(self, path, table_name, drop_old_table=False):
        with psycopg2.connect(**self.conn_dict) as conn:
            first_lines_csv = next(pd.read_csv(path, chunksize=1000))

            if drop_old_table:
                with conn.cursor() as cursor:
                    TransactionManager.execute_command(cursor, f'DROP TABLE IF EXISTS {table_name}', conn)

            first_lines_csv.to_sql(table_name, self.engine, index=False, if_exists='fail')
            with conn.cursor() as cursor:
                TransactionManager.execute_command(cursor, f'DELETE FROM {table_name};', conn)

    def stream_from_file_to_psql(self, path, table_name):
        with psycopg2.connect(**self.conn_dict) as conn:
            with open(path, 'r') as f:
                next(f)  # skip the header
                with conn.cursor() as cursor:
                    try:
                        cursor.copy_from(f, table_name, sep=",", null='')
                        conn.commit()
                    except Exception as e:
                        print(f'Failed to ingest {path.stem}: {e}')
