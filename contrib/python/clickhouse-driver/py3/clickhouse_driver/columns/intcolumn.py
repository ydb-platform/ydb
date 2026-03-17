
from .exceptions import ColumnTypeMismatchException
from .base import FormatColumn
from .largeint import (
    int128_from_quads, int128_to_quads, uint128_from_quads, uint128_to_quads,
    int256_from_quads, int256_to_quads, uint256_from_quads, uint256_to_quads
)


class IntColumn(FormatColumn):
    py_types = (int, )
    int_size = None

    def __init__(self, types_check=False, **kwargs):
        super(IntColumn, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            self.mask = (1 << 8 * self.int_size) - 1

            # Chop only bytes that fit current type.
            # ctypes.c_intXX is slower.
            def before_write_items(items, nulls_map=None):
                # TODO: cythonize
                null_value = self.null_value

                for i, item in enumerate(items):
                    if nulls_map and nulls_map[i]:
                        items[i] = null_value
                        continue

                    if item >= 0:
                        sign = 1
                    else:
                        sign = -1
                        item = -item

                    items[i] = sign * (item & self.mask)

            self.before_write_items = before_write_items


class UIntColumn(IntColumn):
    def __init__(self, types_check=False, **kwargs):
        super(UIntColumn, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            def check_item(value):
                if value < 0:
                    raise ColumnTypeMismatchException(value)

            self.check_item = check_item


class Int8Column(IntColumn):
    ch_type = 'Int8'
    format = 'b'
    int_size = 1


class Int16Column(IntColumn):
    ch_type = 'Int16'
    format = 'h'
    int_size = 2


class Int32Column(IntColumn):
    ch_type = 'Int32'
    format = 'i'
    int_size = 4


class Int64Column(IntColumn):
    ch_type = 'Int64'
    format = 'q'
    int_size = 8


class UInt8Column(UIntColumn):
    ch_type = 'UInt8'
    format = 'B'
    int_size = 1


class UInt16Column(UIntColumn):
    ch_type = 'UInt16'
    format = 'H'
    int_size = 2


class UInt32Column(UIntColumn):
    ch_type = 'UInt32'
    format = 'I'
    int_size = 4


class UInt64Column(UIntColumn):
    ch_type = 'UInt64'
    format = 'Q'
    int_size = 8


class LargeIntColumn(IntColumn):
    format = 'Q'  # We manually deal with sign in read/write.
    factor = None

    to_quads = None
    from_quads = None

    def write_items(self, items, buf):
        n_items = len(items)

        s = self.make_struct(self.factor * n_items)
        uint_64_pairs = self.to_quads(items, n_items)

        buf.write(s.pack(*uint_64_pairs))

    def read_items(self, n_items, buf):
        s = self.make_struct(self.factor * n_items)
        items = s.unpack(buf.read(s.size))

        return self.from_quads(items, n_items)


class Int128Column(LargeIntColumn):
    ch_type = 'Int128'
    int_size = 16
    factor = 2

    to_quads = int128_to_quads
    from_quads = int128_from_quads


class UInt128Column(LargeIntColumn):
    ch_type = 'UInt128'
    int_size = 16
    factor = 2

    to_quads = uint128_to_quads
    from_quads = uint128_from_quads


class Int256Column(LargeIntColumn):
    ch_type = 'Int256'
    int_size = 32
    factor = 4

    to_quads = int256_to_quads
    from_quads = int256_from_quads


class UInt256Column(LargeIntColumn):
    ch_type = 'UInt256'
    int_size = 32
    factor = 4

    to_quads = uint256_to_quads
    from_quads = uint256_from_quads
