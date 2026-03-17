import h5py
import time
import os
import logging
from scipy.sparse import coo_matrix, csr_matrix
import numpy as np

from implicit.datasets import _download


log = logging.getLogger("implicit")


URL = 'https://github.com/benfred/recommender_data/releases/download/v1.0/sketchfab.hdf5'


def get_sketchfab():
    """ Returns the sketchfab dataset, downloading locally if necessary.

    This dataset contains about 632K likes from 62K users on 28k items collected
    from the sketchfab website, as described here:
    http://blog.ethanrosenthal.com/2016/10/09/likes-out-guerilla-dataset/

    Returns a tuple of (items, users, likes) where likes is a CSR matrix """

    filename = os.path.join(_download.LOCAL_CACHE_DIR, "sketchfab.hdf5")
    if not os.path.isfile(filename):
        log.info("Downloading dataset to '%s'", filename)
        _download.download_file(URL, filename)
    else:
        log.info("Using cached dataset at '%s'", filename)

    with h5py.File(filename, 'r') as f:
        m = f.get('item_user_likes')
        plays = csr_matrix((m.get('data'), m.get('indices'), m.get('indptr')))
        return np.array(f['item']), np.array(f['user']), plays


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
    """ Reads the original dataset PSV as a pandas dataframe """
    import pandas

    # read in triples of user/artist/playcount from the input dataset
    # get a model based off the input params
    start = time.time()
    log.debug("reading data from %s", filename)
    data = pandas.read_csv(filename, delimiter='|', quotechar='\\')

    # map each artist and user to a unique numeric value
    data['uid'] = data['uid'].astype("category")
    data['mid'] = data['mid'].astype("category")

    # store as a CSR matrix
    log.debug("read data file in %s", time.time() - start)
    return data


def _hfd5_from_dataframe(data, outputfilename):
    items = data['mid'].cat.codes.copy()
    users = data['uid'].cat.codes.copy()
    values = np.ones(len(items)).astype(np.float32)

    # create a sparse matrix of all the item/users/likes
    likes = coo_matrix((values, (items, users))).astype(np.float32).tocsr()

    with h5py.File(outputfilename, "w") as f:
        g = f.create_group('item_user_likes')
        g.create_dataset("data", data=likes.data)
        g.create_dataset("indptr", data=likes.indptr)
        g.create_dataset("indices", data=likes.indices)

        dt = h5py.special_dtype(vlen=str)
        item = list(data['mid'].cat.categories)
        dset = f.create_dataset('item', (len(item),), dtype=dt)
        dset[:] = item

        user = list(data['uid'].cat.categories)
        dset = f.create_dataset('user', (len(user),), dtype=dt)
        dset[:] = user
