import numpy as np

from .base import NumpyColumn


class NumpyDateColumn(NumpyColumn):
    dtype = np.dtype(np.uint16)
    ch_type = 'Date'

    def read_items(self, n_items, buf):
        data = super(NumpyDateColumn, self).read_items(n_items, buf)
        return data.astype('datetime64[D]')
