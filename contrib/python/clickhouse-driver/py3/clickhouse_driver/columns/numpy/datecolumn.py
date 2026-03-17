import numpy as np

from .base import NumpyColumn


class NumpyDateColumn(NumpyColumn):
    dtype = np.dtype(np.uint16)
    ch_type = 'Date'

    null_value = np.datetime64(0, 'Y')

    def read_items(self, n_items, buf):
        data = super(NumpyDateColumn, self).read_items(n_items, buf)
        return data.astype('datetime64[D]')

    def write_items(self, items, buf):
        super(NumpyDateColumn, self).write_items(
            items.astype('datetime64[D]'), buf
        )
