from ... import errors
from ..arraycolumn import create_array_column
from .datecolumn import NumpyDateColumn
from .datetimecolumn import create_numpy_datetime_column
from ..decimalcolumn import create_decimal_column
from ..enumcolumn import create_enum_column
from .floatcolumn import NumpyFloat32Column, NumpyFloat64Column
from .intcolumn import (
    NumpyInt8Column, NumpyInt16Column, NumpyInt32Column, NumpyInt64Column,
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column
)
from .lowcardinalitycolumn import create_numpy_low_cardinality_column
from ..nothingcolumn import NothingColumn
from ..nullcolumn import NullColumn
# from .nullablecolumn import create_nullable_column
from ..simpleaggregatefunctioncolumn import (
    create_simple_aggregate_function_column
)
from .stringcolumn import create_string_column
from ..tuplecolumn import create_tuple_column
from ..uuidcolumn import UUIDColumn
from ..intervalcolumn import (
    IntervalYearColumn, IntervalMonthColumn, IntervalWeekColumn,
    IntervalDayColumn, IntervalHourColumn, IntervalMinuteColumn,
    IntervalSecondColumn
)
from ..ipcolumn import IPv4Column, IPv6Column

column_by_type = {c.ch_type: c for c in [
    NumpyDateColumn,
    NumpyFloat32Column, NumpyFloat64Column,
    NumpyInt8Column, NumpyInt16Column, NumpyInt32Column, NumpyInt64Column,
    NumpyUInt8Column, NumpyUInt16Column, NumpyUInt32Column, NumpyUInt64Column,
    NothingColumn, NullColumn, UUIDColumn,
    IntervalYearColumn, IntervalMonthColumn, IntervalWeekColumn,
    IntervalDayColumn, IntervalHourColumn, IntervalMinuteColumn,
    IntervalSecondColumn, IPv4Column, IPv6Column
]}


def get_numpy_column_by_spec(spec, column_options):
    def create_column_with_options(x):
        return get_numpy_column_by_spec(x, column_options)

    if spec == 'String' or spec.startswith('FixedString'):
        return create_string_column(spec, column_options)

    elif spec.startswith('Enum'):
        return create_enum_column(spec, column_options)

    elif spec.startswith('DateTime'):
        return create_numpy_datetime_column(spec, column_options)

    elif spec.startswith('Decimal'):
        return create_decimal_column(spec, column_options)

    elif spec.startswith('Array'):
        return create_array_column(spec, create_column_with_options)

    elif spec.startswith('Tuple'):
        return create_tuple_column(spec, create_column_with_options)

    # elif spec.startswith('Nullable'):
    #     return create_nullable_column(spec, create_column_with_options)

    elif spec.startswith('LowCardinality'):
        return create_numpy_low_cardinality_column(spec,
                                                   create_column_with_options)

    elif spec.startswith('SimpleAggregateFunction'):
        return create_simple_aggregate_function_column(
            spec, create_column_with_options)

    else:
        try:
            cls = column_by_type[spec]
            return cls(**column_options)

        except KeyError as e:
            raise errors.UnknownTypeError('Unknown type {}'.format(e.args[0]))
