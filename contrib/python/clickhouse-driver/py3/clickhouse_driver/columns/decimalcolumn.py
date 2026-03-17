from decimal import Decimal, localcontext

from .base import FormatColumn
from .exceptions import ColumnTypeMismatchException
from .intcolumn import Int128Column, Int256Column


class DecimalColumn(FormatColumn):
    py_types = (Decimal, float, int)
    max_precision = None

    def __init__(self, precision, scale, types_check=False, **kwargs):
        self.precision = precision
        self.scale = scale
        super(DecimalColumn, self).__init__(**kwargs)

        if types_check:
            def check_item(value):
                parts = str(value).split('.')
                int_part = parts[0]

                if len(int_part) > precision:
                    raise ColumnTypeMismatchException(value)

            self.check_item = check_item

    def after_read_items(self, items, nulls_map=None):
        if self.scale >= 1:
            scale = 10 ** self.scale

            if nulls_map is None:
                return tuple(Decimal(item) / scale for item in items)
            else:
                return tuple(
                    (None if is_null else Decimal(items[i]) / scale)
                    for i, is_null in enumerate(nulls_map)
                )
        else:
            if nulls_map is None:
                return tuple(Decimal(item) for item in items)
            else:
                return tuple(
                    (None if is_null else Decimal(items[i]))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        if self.scale >= 1:
            scale = 10 ** self.scale

            for i, item in enumerate(items):
                if nulls_map and nulls_map[i]:
                    items[i] = null_value
                else:
                    items[i] = int(Decimal(str(item)) * scale)

        else:
            for i, item in enumerate(items):
                if nulls_map and nulls_map[i]:
                    items[i] = null_value
                else:
                    items[i] = int(Decimal(str(item)))

    # Override default precision to the maximum supported by underlying type.
    def _write_data(self, items, buf):
        with localcontext() as ctx:
            ctx.prec = self.max_precision
            super(DecimalColumn, self)._write_data(items, buf)

    def _read_data(self, n_items, buf, nulls_map=None):
        with localcontext() as ctx:
            ctx.prec = self.max_precision
            return super(DecimalColumn, self)._read_data(
                n_items, buf, nulls_map=nulls_map
            )


class Decimal32Column(DecimalColumn):
    format = 'i'
    max_precision = 9


class Decimal64Column(DecimalColumn):
    format = 'q'
    max_precision = 18


class Decimal128Column(DecimalColumn, Int128Column):
    max_precision = 38


class Decimal256Column(DecimalColumn, Int256Column):
    max_precision = 76


def create_decimal_column(spec, column_options):
    precision, scale = spec[8:-1].split(',')
    precision, scale = int(precision), int(scale)

    # Maximum precisions for underlying types are:
    # Int32    10**9
    # Int64   10**18
    # Int128  10**38
    # Int256  10**76
    if precision <= 9:
        cls = Decimal32Column
    elif precision <= 18:
        cls = Decimal64Column
    elif precision <= 38:
        cls = Decimal128Column
    else:
        cls = Decimal256Column

    return cls(precision, scale, **column_options)
