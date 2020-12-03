from nyc_taxi.etl.psql_tools import TransactionManager

TABLE_NAME = 'green_taxi_records'


def generate_time_indices(table_name=TABLE_NAME):
    tm = TransactionManager()
    tm.create_index_on_column('lpep_dropoff_datetime', table_name)
    tm.create_index_on_column('lpep_pickup_datetime', table_name)
