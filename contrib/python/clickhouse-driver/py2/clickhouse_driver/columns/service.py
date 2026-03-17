from .. import errors
from .arraycolumn import create_array_column
from .datecolumn import DateColumn
from .datetimecolumn import create_datetime_column
from .decimalcolumn import create_decimal_column
from . import exceptions as column_exceptions
from .enumcolumn import create_enum_column
from .floatcolumn import Float32Column, Float64Column
from .intcolumn import (
    Int8Column, Int16Column, Int32Column, Int64Column,
    UInt8Column, UInt16Column, UInt32Column, UInt64Column
)
from .lowcardinalitycolumn import create_low_cardinality_column
from .nothingcolumn import NothingColumn
from .nullcolumn import NullColumn
from .nullablecolumn import create_nullable_column
from .simpleaggregatefunctioncolumn import (
    create_simple_aggregate_function_column
)
from .stringcolumn import create_string_column
from .tuplecolumn import create_tuple_column
from .uuidcolumn import UUIDColumn
from .intervalcolumn import (
    IntervalYearColumn, IntervalMonthColumn, IntervalWeekColumn,
    IntervalDayColumn, IntervalHourColumn, IntervalMinuteColumn,
    IntervalSecondColumn
)
from .ipcolumn import IPv4Column, IPv6Column


column_by_type = {c.ch_type: c for c in [
    DateColumn, Float32Column, Float64Column,
    Int8Column, Int16Column, Int32Column, Int64Column,
    UInt8Column, UInt16Column, UInt32Column, UInt64Column,
    NothingColumn, NullColumn, UUIDColumn,
    IntervalYearColumn, IntervalMonthColumn, IntervalWeekColumn,
    IntervalDayColumn, IntervalHourColumn, IntervalMinuteColumn,
    IntervalSecondColumn, IPv4Column, IPv6Column
]}


def get_column_by_spec(spec, column_options):
    context = column_options['context']
    use_numpy = context.client_settings['use_numpy'] if context else False

    if use_numpy:
        from .numpy.service import get_numpy_column_by_spec
        return get_numpy_column_by_spec(spec, column_options)

    def create_column_with_options(x):
        return get_column_by_spec(x, column_options)

    if spec == 'String' or spec.startswith('FixedString'):
        return create_string_column(spec, column_options)

    elif spec.startswith('Enum'):
        return create_enum_column(spec, column_options)

    elif spec.startswith('DateTime'):
        return create_datetime_column(spec, column_options)

    elif spec.startswith('Decimal'):
        return create_decimal_column(spec, column_options)

    elif spec.startswith('Array'):
        return create_array_column(spec, create_column_with_options)

    elif spec.startswith('Tuple'):
        return create_tuple_column(spec, create_column_with_options)

    elif spec.startswith('Nullable'):
        return create_nullable_column(spec, create_column_with_options)

    elif spec.startswith('LowCardinality'):
        return create_low_cardinality_column(spec, create_column_with_options)

    elif spec.startswith('SimpleAggregateFunction'):
        return create_simple_aggregate_function_column(
            spec, create_column_with_options)

    else:
        try:
            cls = column_by_type[spec]
            return cls(**column_options)

        except KeyError as e:
            raise errors.UnknownTypeError('Unknown type {}'.format(e.args[0]))


def read_column(context, column_spec, n_items, buf):
    column_options = {'context': context}
    column = get_column_by_spec(column_spec, column_options)
    column.read_state_prefix(buf)
    return column.read_data(n_items, buf)


def write_column(context, column_name, column_spec, items, buf,
                 types_check=False):
    column_options = {
        'context': context,
        'types_check': types_check
    }
    column = get_column_by_spec(column_spec, column_options)

    try:
        column.write_state_prefix(buf)
        column.write_data(items, buf)

    except column_exceptions.ColumnTypeMismatchException as e:
        raise errors.TypeMismatchError(
            'Type mismatch in VALUES section. '
            'Expected {} got {}: {} for column "{}".'.format(
                column_spec, type(e.args[0]), e.args[0], column_name
            )
        )

    except (column_exceptions.StructPackException, OverflowError) as e:
        error = e.args[0]
        raise errors.TypeMismatchError(
            'Type mismatch in VALUES section. '
            'Repeat query with types_check=True for detailed info. '
            'Column {}: {}'.format(
                column_name, str(error)
            )
        )
