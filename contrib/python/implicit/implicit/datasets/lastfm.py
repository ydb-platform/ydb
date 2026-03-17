import h5py
import time
import os
import logging
from scipy.sparse import coo_matrix, csr_matrix
import numpy as np

from implicit.datasets import _download


log = logging.getLogger("implicit")


URL = 'https://github.com/benfred/recommender_data/releases/download/v1.0/lastfm_360k.hdf5'


def get_lastfm():
    """ Returns the lastfm360k dataset, downloading locally if necessary.
    Returns a tuple of (artistids, userids, plays) where plays is a CSR matrix """

    filename = os.path.join(_download.LOCAL_CACHE_DIR, "lastfm_360k.hdf5")
    if not os.path.isfile(filename):
        log.info("Downloading dataset to '%s'", filename)
        _download.download_file(URL, filename)
    else:
        log.info("Using cached dataset at '%s'", filename)

    with h5py.File(filename, 'r') as f:
        m = f.get('artist_user_plays')
        plays = csr_matrix((m.get('data'), m.get('indices'), m.get('indptr')))
        return np.array(f['artist']), np.array(f['user']), plays


def generate_dataset(filename, outputfilename):
    """ Generates a hdf5 lastfm datasetfile from the raw datafiles found at:
    http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html

    You shouldn't have to run this yourself, and can instead just download the
    output using the 'get_lastfm' funciton./

    Note there are some invalid entries in this dataset, running
    this function will clean it up so pandas can read it:
    https://github.com/benfred/bens-blog-code/blob/master/distance-metrics/musicdata.py#L39
    """
    data = _read_dataframe(filename)
    _hfd5_from_dataframe(data, outputfilename)


def _read_dataframe(filename):
    """ Reads the original dataset TSV as a pandas dataframe """
    # delay importing this to avoid another dependency
    import pandas

    # read in triples of user/artist/playcount from the input dataset
    # get a model based off the input params
    start = time.time()
    log.debug("reading data from %s", filename)
    data = pandas.read_table(filename,
                             usecols=[0, 2, 3],
                             names=['user', 'artist', 'plays'],
                             na_filter=False)

    # map each artist and user to a unique numeric value
    data['user'] = data['user'].astype("category")
    data['artist'] = data['artist'].astype("category")

    # store as a CSR matrix
    log.debug("read data file in %s", time.time() - start)

    return data


def _hfd5_from_dataframe(data, outputfilename):
    # create a sparse matrix of all the users/plays
    plays = coo_matrix((data['plays'].astype(np.float32),
                       (data['artist'].cat.codes.copy(),
                        data['user'].cat.codes.copy()))).tocsr()

    with h5py.File(outputfilename, "w") as f:
        g = f.create_group('artist_user_plays')
        g.create_dataset("data", data=plays.data)
        g.create_dataset("indptr", data=plays.indptr)
        g.create_dataset("indices", data=plays.indices)

        dt = h5py.special_dtype(vlen=str)
        artist = list(data['artist'].cat.categories)
        dset = f.create_dataset('artist', (len(artist),), dtype=dt)
        dset[:] = artist

        user = list(data['user'].cat.categories)
        dset = f.create_dataset('user', (len(user),), dtype=dt)
        dset[:] = user
