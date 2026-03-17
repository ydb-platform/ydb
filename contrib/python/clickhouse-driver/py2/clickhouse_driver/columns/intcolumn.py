
from ..util import compat
from .exceptions import ColumnTypeMismatchException
from .base import FormatColumn


class IntColumn(FormatColumn):
    py_types = compat.integer_types
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
