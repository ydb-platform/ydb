import h5py
import time
import os
import logging
from scipy.sparse import coo_matrix, csr_matrix
import numpy as np

from implicit.datasets import _download


log = logging.getLogger("implicit")


URL = 'https://github.com/benfred/recommender_data/releases/download/v1.0/reddit.hdf5'


def get_reddit():
    """ Returns the reddit dataset, downloading locally if necessary.

    This dataset was released here:
    https://www.reddit.com/r/redditdev/comments/dtg4j/want_to_help_reddit_build_a_recommender_a_public/
    and contains 23M up/down votes from 44K users on 3.4M links.

    Returns a CSR matrix of (item, user, rating """

    filename = os.path.join(_download.LOCAL_CACHE_DIR, "reddit.hdf5")
    if not os.path.isfile(filename):
        log.info("Downloading dataset to '%s'", filename)
        _download.download_file(URL, filename)
    else:
        log.info("Using cached dataset at '%s'", filename)

    with h5py.File(filename, 'r') as f:
        m = f.get('item_user_ratings')
        return csr_matrix((m.get('data'), m.get('indices'), m.get('indptr')))


def generate_dataset(filename, outputfilename):
    """ Generates a hdf5 reddit datasetfile from the raw datafiles found at:
    https://www.reddit.com/r/redditdev/comments/dtg4j/want_to_help_reddit_build_a_recommender_a_public/

    You shouldn't have to run this yourself, and can instead just download the
    output using the 'get_reddit' funciton.
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
    data = pandas.read_table(filename, usecols=[0, 1, 3], names=['user', 'item', 'rating'])

    # map each artist and user to a unique numeric value
    data['user'] = data['user'].astype("category")
    data['item'] = data['item'].astype("category")

    # store as a CSR matrix
    log.debug("read data file in %s", time.time() - start)
    return data


def _hfd5_from_dataframe(data, outputfilename):
    ratings = coo_matrix((data['rating'].astype(np.float32),
                         (data['item'].cat.codes.copy(),
                          data['user'].cat.codes.copy()))).tocsr()
    print(repr(ratings))
    print(repr(ratings.indices))
    print(repr(ratings.indptr))

    with h5py.File(outputfilename, "w") as f:
        g = f.create_group('item_user_ratings')
        g.create_dataset("data", data=ratings.data)
        g.create_dataset("indptr", data=ratings.indptr)
        g.create_dataset("indices", data=ratings.indices)
        # Note: not saving itemid strings or userid strings here
        # they are just salted hashes, and only lead to  bloat/slowness for no benefit.
