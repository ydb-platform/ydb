import numpy as np
import pandas as pd

from ..base import Column


class NumpyColumn(Column):
    dtype = None

    normalize_null_value = True

    def read_items(self, n_items, buf):
        data = buf.read(n_items * self.dtype.itemsize)
        return np.frombuffer(data, self.dtype.newbyteorder('<'), n_items)

    def write_items(self, items, buf):
        buf.write(items.astype(self.dtype.newbyteorder('<')).tobytes())

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        nulls_map = self._get_nulls_map(items)
        buf.write(s.pack(*nulls_map))

    def _get_nulls_map(self, items):
        return [bool(x) for x in pd.isnull(items)]

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            items = np.array(items, dtype=object)
            np.place(items, nulls_map, None)

        return items

    def prepare_items(self, items):
        nulls_map = pd.isnull(items)

        # Always replace null values to null_value for proper inserts into
        # non-nullable columns.
        if isinstance(items, np.ndarray) and self.normalize_null_value:
            items = np.array(items)
            np.place(items, nulls_map, self.null_value)

        return items
