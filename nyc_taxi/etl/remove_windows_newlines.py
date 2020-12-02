from pathlib import Path

from tqdm import tqdm
from multiprocessing import Pool, cpu_count

DATA_PATHS = list(Path('../data/green_taxi').glob('*.csv'))


def remove_new_lines(paths=None, processes_limit=12):
    if paths is None:
        paths = DATA_PATHS
    with Pool(min(cpu_count(), processes_limit)) as pool:
        list(tqdm(pool.imap(remove_new_lines_single_file, paths), total=len(paths)))


def remove_new_lines_single_file(path):
    with open(path, 'r') as f:
        updated_text = f.read().replace(r'\r', '')
        updated_text = '\n'.join(list(filter(
            lambda x: x.strip(),
            updated_text.split('\n'))))
    with open(path, 'w') as f:
        f.write(updated_text)
