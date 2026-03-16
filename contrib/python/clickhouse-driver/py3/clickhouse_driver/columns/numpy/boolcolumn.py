import numpy as np

from .base import NumpyColumn


class NumpyBoolColumn(NumpyColumn):
    dtype = np.dtype(np.bool_)
    ch_type = 'Bool'
