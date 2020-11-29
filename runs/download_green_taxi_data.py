from nyc_taxi.etl.download_data_from_nyc_gov import download_all_data


if __name__ == '__main__':
    download_all_data('../data/green_taxi')
