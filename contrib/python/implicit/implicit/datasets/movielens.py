import h5py
import os
import logging
from scipy.sparse import coo_matrix, csr_matrix
import numpy as np

from implicit.datasets import _download


log = logging.getLogger("implicit")


URL_BASE = 'https://github.com/benfred/recommender_data/releases/download/v1.0/'


def get_movielens(variant="20m"):
    """ Gets movielens datasets

    Parameters
    ---------
    variant : string
        Which version of the movielens dataset to download. Should be one of '20m', '10m',
        '1m' or '100k'.

    Returns
    -------
    movies : ndarray
        An array of the movie titles.
    ratings : csr_matrix
        A sparse matrix where the row is the movieId, the column is the userId and the value is
        the rating.
    """
    filename = "movielens_%s.hdf5" % variant

    path = os.path.join(_download.LOCAL_CACHE_DIR, filename)
    if not os.path.isfile(path):
        log.info("Downloading dataset to '%s'", path)
        _download.download_file(URL_BASE + filename, path)
    else:
        log.info("Using cached dataset at '%s'", path)

    with h5py.File(path, 'r') as f:
        m = f.get('movie_user_ratings')
        plays = csr_matrix((m.get('data'), m.get('indices'), m.get('indptr')))
        return np.array(f['movie']), plays


def generate_dataset(path, variant='20m', outputpath="."):
    """ Generates a hdf5 movielens datasetfile from the raw datafiles found at:
    https://grouplens.org/datasets/movielens/20m/

    You shouldn't have to run this yourself, and can instead just download the
    output using the 'get_movielens' funciton./
    """
    filename = os.path.join(outputpath, "movielens_%s.hdf5" % variant)

    if variant == '20m':
        ratings, movies = _read_dataframes_20M(path)
    elif variant == '100k':
        ratings, movies = _read_dataframes_100k(path)
    else:
        ratings, movies = _read_dataframes(path)

    _hfd5_from_dataframe(ratings, movies, filename)


def _read_dataframes_20M(path):
    """ reads in the movielens 20M"""
    import pandas

    ratings = pandas.read_csv(os.path.join(path, "ratings.csv"))
    movies = pandas.read_csv(os.path.join(path, "movies.csv"))

    return ratings, movies


def _read_dataframes_100k(path):
    """ reads in the movielens 100k dataset"""
    import pandas

    ratings = pandas.read_table(os.path.join(path, "u.data"),
                                names=['userId', 'movieId', 'rating', 'timestamp'])

    movies = pandas.read_csv(os.path.join(path, "u.item"),
                             names=['movieId', 'title'],
                             usecols=[0, 1],
                             delimiter='|',
                             encoding='ISO-8859-1')

    return ratings, movies


def _read_dataframes(path):
    import pandas
    ratings = pandas.read_csv(os.path.join(path, "ratings.dat"),  delimiter="::",
                              names=['userId', 'movieId', 'rating', 'timestamp'])

    movies = pandas.read_table(os.path.join(path, "movies.dat"), delimiter="::",
                               names=['movieId', 'title', 'genres'])
    return ratings, movies


def _hfd5_from_dataframe(ratings, movies, outputfilename):
    # transform ratings dataframe into a sparse matrix
    m = coo_matrix((ratings['rating'].astype(np.float32),
                   (ratings['movieId'], ratings['userId']))).tocsr()

    with h5py.File(outputfilename, "w") as f:
        # write out the ratings matrix
        g = f.create_group('movie_user_ratings')
        g.create_dataset("data", data=m.data)
        g.create_dataset("indptr", data=m.indptr)
        g.create_dataset("indices", data=m.indices)

        # write out the titles as a numpy array
        titles = np.empty(shape=(movies.movieId.max()+1,), dtype=np.object)
        titles[movies.movieId] = movies.title
        dt = h5py.special_dtype(vlen=str)
        dset = f.create_dataset('movie', (len(titles),), dtype=dt)
        dset[:] = titles
