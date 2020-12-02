from pathlib import Path

from tqdm import tqdm

from nyc_taxi.etl.psql_tools import TransactionManager

DATA_FOLDER = '../data/green_taxi_unified_columns'
TABLE_NAME = 'green_taxi_records'


def insert_data_to_psql(data_folder=DATA_FOLDER, table_name=TABLE_NAME):
    data_folder = Path(data_folder)
    files_to_ingest = list(data_folder.glob('*.csv'))

    tm = TransactionManager()

    tm.create_table_from_csv(files_to_ingest[0], table_name, drop_old_table=True)
    for path in tqdm(files_to_ingest):
        tm.stream_from_file_to_psql(path, table_name)
