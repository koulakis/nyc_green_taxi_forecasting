from pathlib import Path
from typing import Union

import pandas as pd
import requests
import time
from tqdm import tqdm


DOWNLOAD_LINK_FORMAT = 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_{year}-{month:02}.csv'
EARLIEST_ENTRY_DATE = pd.Timestamp('2013-08')
DEFAULT_LATEST_ENTRY_DATE = pd.Timestamp('2020-07')


def _download_entry(url, output_dir, wait_between_downloads_sec):
    output_dir = Path(output_dir)

    try:
        filename = Path(url).name
        response = requests.get(url, allow_redirects=True)

        with open(output_dir / filename, 'wb') as f:
            f.write(response.content)
    except Exception as e:
        print(f'Failed to download from the url: {url}')
        print(e)
    finally:
        time.sleep(wait_between_downloads_sec)


def _download_multiple_entries(urls, output_dir, wait_between_downloads_sec):
    for url in tqdm(urls):
        _download_entry(url, output_dir, wait_between_downloads_sec)

    print('Download completed.')


def download_all_data(
        output_dir: Union[str, Path],
        earliest_entry_date: pd.Timestamp = EARLIEST_ENTRY_DATE,
        latest_entry_date: pd.Timestamp = DEFAULT_LATEST_ENTRY_DATE,
        download_link_format: str = DOWNLOAD_LINK_FORMAT,
        wait_between_downloads_sec: int = 5):
    """Download all the green taxi datasets as csv files in the given output directory.

    Args:
        output_dir: the directory to save the csv files to
        earliest_entry_date: first date that green taxi data is available
        latest_entry_date: last day that green taxi data is available. Might want to pass this manually if new data
            appears in the future
        download_link_format: the format of the download links
        wait_between_downloads_sec: small waiting time between file downloads to avoid spamming nyc.gov
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    entry_date_range = pd.date_range(earliest_entry_date, latest_entry_date, freq='M')
    download_links = [download_link_format.format(year=date.year, month=date.month) for date in entry_date_range]

    _download_multiple_entries(download_links, output_dir, wait_between_downloads_sec)
