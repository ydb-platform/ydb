import logging
import os

import numpy as np


def nonzeros(m, row):
    """ returns the non zeroes of a row in csr_matrix """
    for index in range(m.indptr[row], m.indptr[row+1]):
        yield m.indices[index], m.data[index]


_checked_blas_config = False


def check_blas_config():
    """ checks to see if using OpenBlas/Intel MKL. If so, warn if the number of threads isn't set
    to 1 (causes severe perf issues when training - can be 10x slower) """
    # don't warn repeatedly
    global _checked_blas_config
    if _checked_blas_config:
        return
    _checked_blas_config = True

    if np.__config__.get_info('openblas_info') and os.environ.get('OPENBLAS_NUM_THREADS') != '1':
        logging.warning("OpenBLAS detected. Its highly recommend to set the environment variable "
                        "'export OPENBLAS_NUM_THREADS=1' to disable its internal multithreading")
    if np.__config__.get_info('blas_mkl_info') and os.environ.get('MKL_NUM_THREADS') != '1':
        logging.warning("Intel MKL BLAS detected. Its highly recommend to set the environment "
                        "variable 'export MKL_NUM_THREADS=1' to disable its internal "
                        "multithreading")


def check_random_state(random_state):
    """Validate the random state.

    Check a random seed or existing numpy RandomState
    and get back an initialized RandomState.

    Parameters
    ----------
    random_state : int, None or RandomState
        The existing RandomState. If None, or an int, will be used
        to seed a new numpy RandomState.
    """
    # if it's an existing random state, pass through
    if isinstance(random_state, np.random.RandomState):
        return random_state
    # otherwise try to initialize a new one, and let it fail through
    # on the numpy side if it doesn't work
    return np.random.RandomState(random_state)
