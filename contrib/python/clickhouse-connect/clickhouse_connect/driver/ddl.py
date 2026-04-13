from typing import NamedTuple, Sequence

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.options import check_arrow


class TableColumnDef(NamedTuple):
    """
    Simplified ClickHouse Table Column definition for DDL
    """
    name: str
    ch_type: ClickHouseType
    expr_type: str = None
    expr: str = None

    @property
    def col_expr(self):
        expr = f'{self.name} {self.ch_type.name}'
        if self.expr_type:
            expr += f' {self.expr_type} {self.expr}'
        return expr


def create_table(table_name: str, columns: Sequence[TableColumnDef], engine: str, engine_params: dict):
    stmt = f"CREATE TABLE {table_name} ({', '.join(col.col_expr for col in columns)}) ENGINE {engine} "
    if engine_params:
        for key, value in engine_params.items():
            stmt += f' {key} {value}'
    return stmt


def _arrow_type_to_ch(arrow_type: "pa.DataType") -> str: # pylint: disable=too-many-return-statements,too-many-branches
    """
    Best-effort mapping from common PyArrow types to ClickHouse type names.

    Covers core scalar and common date/time/timestamp types. For anything unknown, we raise, so the
    caller is aware that the automatic mapping is not implemented for that Arrow type.
    """
    pa = check_arrow()

    pat = pa.types

    # Signed ints
    if pat.is_int8(arrow_type):
        return 'Int8'
    if pat.is_int16(arrow_type):
        return 'Int16'
    if pat.is_int32(arrow_type):
        return 'Int32'
    if pat.is_int64(arrow_type):
        return 'Int64'

    # Unsigned ints
    if pat.is_uint8(arrow_type):
        return 'UInt8'
    if pat.is_uint16(arrow_type):
        return 'UInt16'
    if pat.is_uint32(arrow_type):
        return 'UInt32'
    if pat.is_uint64(arrow_type):
        return 'UInt64'

    # Floats
    if pat.is_float16(arrow_type) or pat.is_float32(arrow_type):
        return 'Float32'
    if pat.is_float64(arrow_type):
        return 'Float64'

    # Boolean
    if pat.is_boolean(arrow_type):
        return 'Bool'

    # Dates
    if pat.is_date32(arrow_type):
        return 'Date32'
    if pat.is_date64(arrow_type):
        return 'DateTime64(3)'

    # Timestamps → DateTime / DateTime64
    if pat.is_timestamp(arrow_type):
        unit = getattr(arrow_type, 'unit', 's')
        tz = getattr(arrow_type, 'tz', None)

        if unit == 's':
            base = 'DateTime'
            if tz:
                return f"DateTime('{tz}')"
            return base

        scale_map = {'ms': 3, 'us': 6, 'ns': 9}
        scale = scale_map.get(unit, 3)
        if tz:
            return f"DateTime64({scale}, '{tz}')"
        return f'DateTime64({scale})'

    # Strings (this covers pa.string(), pa.large_string())
    if pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
        return 'String'

    # for any currently unsupported type, we raise so it’s clear that
    # this Arrow type isn’t supported by the helper yet.
    raise TypeError(f'Unsupported Arrow type for automatic mapping: {arrow_type!r}')


class _DDLType:
    """
    Minimal helper used to satisfy TableColumnDef.ch_type.

    create_table() only needs ch_type.name when building the DDL string,
    so we'll wrap the ClickHouse type name in this tiny object instead of
    constructing full ClickHouseType instances here.
    """
    def __init__(self, name: str):
        self.name = name


def arrow_schema_to_column_defs(schema: "pa.Schema") -> list[TableColumnDef]:
    """
    Convert a PyArrow Schema into a list of TableColumnDef objects.
    
    This helper uses an *optimistic non-null* strategy: it always produces
    non-nullable ClickHouse types, even though Arrow fields are nullable by
    default.

    Note that if the user inserts a table with nulls into a non-Nullable column,
    ClickHouse will silently convert those nulls to default values due to the default
    server setting input_format_null_as_default=1 and current lack of client-side
    validation on arrow inserts.
    """
    pa = check_arrow()

    if not isinstance(schema, pa.Schema):
        raise TypeError(f'Expected pyarrow.Schema, got {type(schema)!r}')

    col_defs: list[TableColumnDef] = []
    for field in schema:
        ch_type_name = _arrow_type_to_ch(field.type)
        col_defs.append(
            TableColumnDef(
                name=field.name,
                ch_type=_DDLType(ch_type_name),
            )
        )
    return col_defs


def create_table_from_arrow_schema(
    table_name: str,
    schema: "pa.Schema",
    engine: str,
    engine_params: dict,
) -> str:
    """
    Helper function to build a CREATE TABLE statement from a PyArrow Schema.

    Internally:
      schema -> arrow_schema_to_column_defs -> create_table(...)
    """
    col_defs = arrow_schema_to_column_defs(schema)
    return create_table(table_name, col_defs, engine, engine_params)
