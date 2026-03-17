from itertools import chain
from struct import Struct

from .base import Column
from .intcolumn import UInt64Column
from ..util.helpers import pairwise


class ArrayColumn(Column):
    """
    Nested arrays written in flatten form after information about their
    sizes (offsets really).
    One element of array of arrays can be represented as tree:
    (0 depth)          [[3, 4], [5, 6]]
                      |               |
    (1 depth)      [3, 4]           [5, 6]
                   |    |           |    |
    (leaf)        3     4          5     6

    Offsets (sizes) written in breadth-first search order. In example above
    following sequence of offset will be written: 4 -> 2 -> 4
    1) size of whole array: 4
    2) size of array 1 in depth=1: 2
    3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4

    After sizes info comes flatten data: 3 -> 4 -> 5 -> 6
    """
    py_types = (list, tuple)

    def __init__(self, nested_column, **kwargs):
        self.init_kwargs = kwargs
        self.size_column = UInt64Column(**kwargs)
        self.nested_column = nested_column
        self._write_depth_0_size = True
        super(ArrayColumn, self).__init__(**kwargs)
        self.null_value = []

    def write_data(self, data, buf):
        # Column of Array(T) is stored in "compact" format and passed to server
        # wrapped into another Array without size of wrapper array.
        self.nested_column = ArrayColumn(
            self.nested_column, **self.init_kwargs
        )
        self.nested_column.nullable = self.nullable
        self.nullable = False
        self._write_depth_0_size = False
        self._write(data, buf)

    def read_data(self, n_rows, buf):
        self.nested_column = ArrayColumn(
            self.nested_column, **self.init_kwargs
        )
        self.nested_column.nullable = self.nullable
        self.nullable = False
        return self._read(n_rows, buf)[0]

    def _write_sizes(self, value, buf):
        nulls_map = []

        column = self
        sizes = [len(value)] if self._write_depth_0_size else []

        while True:
            nested_column = column.nested_column
            if not isinstance(nested_column, ArrayColumn):
                if column.nullable:
                    nulls_map = [x is None for x in value]
                break

            offset = 0
            new_value = []
            for x in value:
                offset += len(x)
                sizes.append(offset)
                new_value.extend(x)

            value = new_value
            column = nested_column

        if nulls_map:
            self._write_nulls_map(nulls_map, buf)

        ns = Struct('<{}Q'.format(len(sizes)))
        buf.write(ns.pack(*sizes))

    def _write_data(self, value, buf):
        if self.nullable:
            value = value or []

        if isinstance(self.nested_column, ArrayColumn):
            value = list(chain.from_iterable(value))

        if value:
            self.nested_column._write_data(value, buf)

    def _write_nulls_data(self, value, buf):
        if self.nullable:
            value = value or []

        if isinstance(self.nested_column, ArrayColumn):
            value = list(chain.from_iterable(value))
            self.nested_column._write_nulls_data(value, buf)
        else:
            if self.nested_column.nullable:
                self.nested_column._write_nulls_map(value, buf)

    def _write(self, value, buf):
        value = self.prepare_items(value)
        self._write_sizes(value, buf)
        self._write_nulls_data(value, buf)
        self._write_data(value, buf)

    def read_state_prefix(self, buf):
        super(ArrayColumn, self).read_state_prefix(buf)

        self.nested_column.read_state_prefix(buf)

    def write_state_prefix(self, buf):
        super(ArrayColumn, self).write_state_prefix(buf)

        self.nested_column.write_state_prefix(buf)

    def _read(self, size, buf):
        slices_series = [[0, size]]
        nested_column = self.nested_column

        cur_level_slice_size = size
        cur_level_slice = None
        while (isinstance(nested_column, ArrayColumn)):
            if cur_level_slice is None:
                cur_level_slice = [0]
            ns = Struct('<{}Q'.format(cur_level_slice_size))
            nested_sizes = ns.unpack(buf.read(ns.size))
            cur_level_slice.extend(nested_sizes)
            slices_series.append(cur_level_slice)
            cur_level_slice = None
            cur_level_slice_size = nested_sizes[-1] if len(nested_sizes) > 0 \
                else 0
            nested_column = nested_column.nested_column

        n_items = cur_level_slice_size if size > 0 else 0
        nulls_map = None
        if nested_column.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)

        data = []
        if n_items:
            data = list(nested_column._read_data(
                n_items, buf, nulls_map=nulls_map
            ))

        # Build nested structure.
        for slices in reversed(slices_series):
            data = [data[begin:end] for begin, end in pairwise(slices)]

        return tuple(data)


def create_array_column(spec, column_by_spec_getter, column_options):
    inner = spec[6:-1]
    return ArrayColumn(column_by_spec_getter(inner), **column_options)
