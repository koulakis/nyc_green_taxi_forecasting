from pathlib import Path
from functools import reduce, partial

from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count


INPUT_FOLDER = '../data/green_taxi'
OUTPUT_FOLDER = '../data/green_taxi_unified_columns'


def unify_columns(
        input_folder=INPUT_FOLDER,
        output_folder=OUTPUT_FOLDER,
        max_nr_processes=12):
    """Ingest all the csv files from a directory to a psql table.

    Args:
        input_folder: the folder containing the original csv files
        output_folder: name of the folder to output the processed files
        max_nr_processes: a limit on the number of processes to avoid exhausting resources
    """
    data_folder = Path(input_folder)
    output_folder = Path(output_folder)
    output_folder.mkdir(exist_ok=True)
    files_to_ingest = list(data_folder.glob('*.csv'))

    all_columns, extra_column_dtypes = get_union_of_all_columns_and_dtypes(files_to_ingest)

    with Pool(processes=min(cpu_count(), max_nr_processes)) as pool:
        list(tqdm(
            pool.imap(
                partial(
                    process_csv,
                    all_columns=all_columns,
                    extra_column_dtypes=extra_column_dtypes,
                    output_folder=output_folder),
                files_to_ingest),
            total=len(files_to_ingest)))


def get_union_of_all_columns_and_dtypes(filepaths):
    def read_header(path):
        with open(path, 'r') as f:
            return f.readline().replace('\n', '').strip().lower()

    headers = pd.DataFrame(dict(path=filepaths, columns=[read_header(p) for p in filepaths]))

    header_union = reduce(
        lambda x, y: x.union(y),
        [set(c.split(',')) for c in headers['columns'].unique()])

    non_shared_column_dtypes = {
        'congestion_surcharge': np.float,
        'dolocationid': pd.Int64Dtype(),
        'dropoff_latitude': np.float,
        'dropoff_longitude': np.float,
        'improvement_surcharge': np.float,
        'pickup_latitude': np.float,
        'pickup_longitude': np.float,
        'pulocationid': pd.Int64Dtype()
    }

    return header_union, non_shared_column_dtypes


def read_csv_ignoring_empty_extra_columns(path):
    with open(path, 'r') as f:
        header = f.readline().strip().split(',')
        first_line = f.readline().strip().split(',')

    extra_columns = len(first_line) - len(header)
    if extra_columns == 0:
        df = pd.read_csv(path)
    else:
        extended_header = header + list(range(extra_columns))
        df = pd.read_csv(path, names=extended_header, skiprows=1, header=None)
        df = df[header]
    return df


def process_csv(path, all_columns, extra_column_dtypes, output_folder):
    output_dir = Path(output_folder)
    path = Path(path)

    df = read_csv_ignoring_empty_extra_columns(path)
    df = df.rename(columns={col: col.lower().strip() for col in df.columns})

    missing_columns = all_columns.difference(df.columns)
    df = df.assign(**{
        col: pd.Series(len(df) * [np.nan], dtype=extra_column_dtypes[col])
        for col in missing_columns})

    column_types = df.dtypes
    integer_columns = list(column_types[column_types == np.int].index)
    (df[sorted(df.columns)]
     .assign(**{
        col: lambda d: d[col].astype(np.float)
        for col in integer_columns})
     .to_csv(output_dir / path.name, index=False))
