import os
try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

from tqdm.auto import tqdm


LOCAL_CACHE_DIR = os.path.join(os.path.expanduser("~"), "implicit_datasets")


def download_file(url, local_filename):
    """ Simple wrapper around urlretrieve that uses tqdm to display a progress
    bar of download progress """
    local_filename = os.path.abspath(local_filename)
    path = os.path.dirname(local_filename)
    if not os.path.isdir(path):
        os.makedirs(path)

    with tqdm(unit='B', unit_scale=True) as progress:
        def report(chunk, chunksize, total):
            progress.total = total
            progress.update(chunksize)
        return urlretrieve(url, local_filename, reporthook=report)
