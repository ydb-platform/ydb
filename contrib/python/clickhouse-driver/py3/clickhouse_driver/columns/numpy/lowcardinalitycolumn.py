from math import log

import numpy as np
import pandas as pd

from ..lowcardinalitycolumn import LowCardinalityColumn
from ...reader import read_binary_uint64
from ...writer import write_binary_int64
from .intcolumn import (
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
)


class NumpyLowCardinalityColumn(LowCardinalityColumn):
    int_types = {
        0: NumpyUInt8Column,
        1: NumpyUInt16Column,
        2: NumpyUInt32Column,
        3: NumpyUInt64Column
    }

    def __init__(self, nested_column, **kwargs):
        super(NumpyLowCardinalityColumn, self).__init__(nested_column,
                                                        **kwargs)

    def _write_data(self, items, buf):
        # Do not write anything for empty column.
        # May happen while writing empty arrays.
        if not len(items):
            return

        # Replace nans with defaults if not nullabe.
        if isinstance(items, np.ndarray) and not self.nested_column.nullable:
            nulls = pd.isnull(items)
            items = np.where(nulls, self.nested_column.null_value, items)

        c = pd.Categorical(items)

        int_type = int(log(len(c.codes), 2) / 8)
        int_column = self.int_types[int_type](**self.init_kwargs)

        serialization_type = self.serialization_type | int_type

        index = c.categories
        keys = c.codes

        if self.nested_column.nullable:
            # First element represents NULL if column is nullable.
            index = index.insert(0, self.nested_column.null_value)
            keys = keys + 1
            # Prevent null map writing. Reset nested column nullable flag.
            self.nested_column.nullable = False

        write_binary_int64(serialization_type, buf)
        write_binary_int64(len(index), buf)

        self.nested_column.write_data(index.to_numpy(items.dtype), buf)
        write_binary_int64(len(items), buf)
        int_column.write_items(keys, buf)

    def _read_data(self, n_items, buf, nulls_map=None):
        if not n_items:
            return tuple()

        serialization_type = read_binary_uint64(buf)

        # Lowest byte contains info about key type.
        key_type = serialization_type & 0xf
        keys_column = self.int_types[key_type](**self.init_kwargs)

        nullable = self.nested_column.nullable
        # Prevent null map reading. Reset nested column nullable flag.
        self.nested_column.nullable = False

        index_size = read_binary_uint64(buf)
        index = self.nested_column.read_data(index_size, buf)

        read_binary_uint64(buf)  # number of keys
        keys = keys_column.read_data(n_items, buf)

        if nullable:
            # Shift all codes by one ("No value" code is -1 for pandas
            # categorical) and drop corresponding first index
            # this is analog of original operation:
            # index = (None, ) + index[1:]
            keys = np.array(keys, dtype='int64')  # deal with possible overflow
            keys = keys - 1
            index = index[1:]
        return pd.Categorical.from_codes(keys, index)


def create_numpy_low_cardinality_column(spec, column_by_spec_getter,
                                        column_options):
    inner = spec[15:-1]
    nested = column_by_spec_getter(inner)
    return NumpyLowCardinalityColumn(nested, **column_options)
