from collections import deque
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
        self.size_column = UInt64Column()
        self.nested_column = nested_column
        self._write_depth_0_size = True
        super(ArrayColumn, self).__init__(**kwargs)

    def write_data(self, data, buf):
        # Column of Array(T) is stored in "compact" format and passed to server
        # wrapped into another Array without size of wrapper array.
        self.nested_column = ArrayColumn(self.nested_column)
        self.nested_column.nullable = self.nullable
        self.nullable = False
        self._write_depth_0_size = False
        self._write(data, buf)

    def read_data(self, rows, buf):
        self.nested_column = ArrayColumn(self.nested_column)
        self.nested_column.nullable = self.nullable
        self.nullable = False
        return self._read(rows, buf)

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
        self._write_sizes(value, buf)
        self._write_nulls_data(value, buf)
        self._write_data(value, buf)

    def read_state_prefix(self, buf):
        return self.nested_column.read_state_prefix(buf)

    def write_state_prefix(self, buf):
        self.nested_column.write_state_prefix(buf)

    def _read(self, size, buf):
        q = deque()
        q.appendleft((self, [size], 0))

        slices_series = []
        slices = []

        cur_depth = 0

        nulls_map = None
        nested_column = self.nested_column
        n_items = 0

        # Read and store info about slices.
        while q:
            column, sizes, depth = q.pop()

            nested_column = column.nested_column

            if cur_depth != depth:
                cur_depth = depth

                slices_series.append(slices)

                # The last element in slice is index(number) of the last
                # element in current level. On the last iteration this
                # represents number of elements in fully flatten array.
                n_items = slices[-1]
                if nested_column.nullable:
                    nulls_map = self._read_nulls_map(n_items, buf)

                slices = []

            if isinstance(nested_column, ArrayColumn):
                slices.append(0)
                prev = 0
                for size in sizes:
                    ns = Struct('<{}Q'.format(size - prev))
                    nested_sizes = ns.unpack(buf.read(ns.size))
                    slices.extend(nested_sizes)
                    prev = size

                    q.appendleft((nested_column, nested_sizes, cur_depth + 1))

        data = []
        if n_items:
            data = list(nested_column._read_data(
                n_items, buf, nulls_map=nulls_map
            ))

        # Build nested structure.
        for slices in reversed(slices_series):
            data = [data[begin:end] for begin, end in pairwise(slices)]

        return tuple(data)


def create_array_column(spec, column_by_spec_getter):
    inner = spec[6:-1]
    return ArrayColumn(column_by_spec_getter(inner))
