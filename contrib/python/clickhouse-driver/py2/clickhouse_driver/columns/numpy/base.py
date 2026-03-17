import numpy as np

from ..base import Column


class NumpyColumn(Column):
    dtype = None

    def read_items(self, n_items, buf):
        data = buf.read(n_items * self.dtype.itemsize)
        return np.frombuffer(data, self.dtype, n_items)

    def write_items(self, items, buf):
        buf.write(items.astype(self.dtype).tobytes())
