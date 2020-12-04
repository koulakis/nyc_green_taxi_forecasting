# New York City green taxi forecasting
This repository provides a way to ingest the New York city green taxi records, a short analysis of the main features of the data and a list of ways to forecast demand.

## Setup
In order to setup the project just run
`pip install setup.py`
In case you want to train deep learning models for forecasting via `darts`, you will need to additionally install `pytorch`.

## Data downloading and injestion to PSQL
Under the directory `runs`, there are some python scripts which execute simple ETL tasks. Those are the following:

- `download_green_taxi_data.py`: Downloads each file from the corresponding link. This is performed sequentially and with a timeout of 5 seconds to avoid getting blocked.
- `remove_windows_newlines.py`: Some of the files contain new lines added by Window programs. The script removes them as they can create problems when loading the files with e.g. `pandas`.
- ` unify_data_columns.py`: Unfortunately, the csv files changed shape and naming several times. In order to unify them in a single table, this script extends the columns of each file to a common list of columns. The files with the new columns are added in a new location.
- `insert_data_to_psql.py`: Imports the unified csvs to a single table in psql. Beware, some dummy credentials are hard-coded and might need to change.
- `generate_time_indices.py`: Generates indices for the date time columns to accelerate querying.

## References
Below are some references to relevant resources and some blogs and tutorials which helped implement the analysis and model setup.

- New York City trip record data: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Kaggle tutorial on geospatial data: https://www.kaggle.com/learn/geospatial-analysis
- Darts time series model zoo: https://unit8co.github.io/darts/generated_api/darts.models.html
- Blog with examples of using darts: https://medium.com/unit8-machine-learning-publication/darts-time-series-made-easy-in-python-5ac2947a8878
- Paper where prophet was introduced: https://peerj.com/preprints/3190v2/
