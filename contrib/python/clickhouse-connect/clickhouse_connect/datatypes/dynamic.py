from collections import namedtuple
from typing import List, Sequence, Collection, Any
from urllib.parse import unquote

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.common import unescape_identifier, first_value, write_uint64
from clickhouse_connect.driver.ctypes import data_conv
from clickhouse_connect.driver.errors import handle_error
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.json_impl import any_to_json

SHARED_DATA_TYPE: ClickHouseType
STRING_DATA_TYPE: ClickHouseType
_JSON_NULL = b'null'
_JSON_NULL_STR = 'null'

json_serialization_format = 0x1

VariantState = namedtuple('VariantState', 'discriminator_node element_states')


def _json_path_segments(path: str) -> List[str]:
    segments = path.split('.')
    if '%' in path:
        return [unquote(segment) for segment in segments]
    return segments


class Variant(ClickHouseType):
    _slots = 'element_types'
    python_type = object

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_types: List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self._name_suffix = f"({', '.join(ch_type.name for ch_type in self.element_types)})"

    @property
    def insert_name(self):
        return 'String'

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext) -> VariantState:
        discriminator_mode = source.read_uint64()
        element_states = [e_type.read_column_prefix(source, ctx) for e_type in self.element_types]
        return VariantState(discriminator_mode, element_states)

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext,
                            read_state: VariantState) -> Sequence:
        return read_variant_column(source, num_rows, ctx, self.element_types, read_state.element_states)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        write_str_values(self, column, dest, ctx)


def read_variant_column(source: ByteSource,
                        num_rows: int,
                        ctx: QueryContext,
                        variant_types: List[ClickHouseType],
                        element_states: List[Any]) -> Sequence:
    v_count = len(variant_types)
    discriminators = source.read_array('B', num_rows)
    # We have to count up how many of each discriminator there are in the block to read the sub columns correctly
    disc_rows = [0] * v_count
    for disc in discriminators:
        if disc != 255:
            disc_rows[disc] += 1
    sub_columns: List[Sequence] = [[]] * v_count
    # Read all the sub-columns
    for ix in range(v_count):
        if disc_rows[ix] > 0:
            sub_columns[ix] = variant_types[ix].read_column_data(source, disc_rows[ix], ctx, element_states[ix])
    # Now we have to walk through each of the discriminators again to assign the correct value from
    # the sub-column to the final result column
    sub_indexes = [0] * v_count
    col = []
    app_col = col.append
    for disc in discriminators:
        if disc == 255:
            app_col(None)
        else:
            app_col(sub_columns[disc][sub_indexes[disc]])
            sub_indexes[disc] += 1
    return col


DynamicState = namedtuple('DynamicState', 'struct_version variant_types variant_states')


def read_dynamic_prefix(_, source: ByteSource, ctx: QueryContext) -> DynamicState:
    struct_version = source.read_uint64()
    if struct_version == 1:
        source.read_leb128()  # max dynamic types, we ignore this value
    elif struct_version != 2:
        raise DataError('Unrecognized dynamic structure version')
    num_variants = source.read_leb128()
    variant_types = [get_from_name(source.read_leb128_str()) for _ in range(num_variants)]
    variant_types.append(STRING_DATA_TYPE)
    if source.read_uint64() != 0:  # discriminator format, currently only 0 is recognized
        raise DataError('Unexpected discriminator format in Variant column prefix')
    variant_states = [e_type.read_column_prefix(source, ctx) for e_type in variant_types]
    return DynamicState(struct_version, variant_types, variant_states)


class Dynamic(ClickHouseType):
    python_type = object
    read_column_prefix = read_dynamic_prefix

    @property
    def insert_name(self):
        return 'String'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        if type_def.keys and type_def.keys[0] == 'max_types':
            self._name_suffix = f'(max_types={type_def.values[0]})'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext,
                            read_state: DynamicState) -> Sequence:
        return read_variant_column(source, num_rows, ctx, read_state.variant_types, read_state.variant_states)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        write_str_values(self, column, dest, ctx)


def json_sample_size(_, sample: Collection) -> int:
    if len(sample) == 0:
        return 0
    total = 0
    for x in sample:
        if isinstance(x, str):
            total += len(x)
        elif x:
            total += len(any_to_json(x))
    return total // len(sample) + 1


def write_json(ch_type: ClickHouseType, column: Sequence, dest: bytearray, ctx: InsertContext):
    if ch_type.nullable:
        dest += bytearray(1 if v is None else 0 for v in column)

    first = first_value(column, ch_type.nullable)
    write_col = column
    encoding = ctx.encoding or ch_type.encoding
    if not isinstance(first, str) and ch_type.write_format(ctx) != 'string':
        to_json = any_to_json
        if ch_type.nullable:
            write_col = [_JSON_NULL if v is None else to_json(v) for v in column]
        else:
            write_col = [to_json(v) for v in column]
        encoding = None
    else:
        write_col = [_JSON_NULL_STR if v is None else v for v in column]

    handle_error(data_conv.write_str_col(write_col, ch_type.nullable, encoding, dest), ctx)


def write_str_values(ch_type: ClickHouseType, column: Sequence, dest: bytearray, ctx: InsertContext):
    encoding = ctx.encoding or ch_type.encoding
    col = [''] * len(column)
    for ix, v in enumerate(column):
        if v is None:
            col[ix] = 'NULL'
        else:
            col[ix] = str(v)
    handle_error(data_conv.write_str_col(col, False, encoding, dest), ctx)


JSONState = namedtuple('JSONState', 'serialize_version dynamic_paths typed_states dynamic_states')


class JSON(ClickHouseType):
    _slots = 'typed_paths', 'typed_types'
    python_type = dict
    valid_formats = 'string', 'native'
    _data_size = json_sample_size
    write_column_data = write_json
    shared_data_type: ClickHouseType
    max_dynamic_paths = 0
    max_dynamic_types = 0
    typed_paths = []
    typed_types = []
    skips = []

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        typed_paths = []
        typed_types = []
        skips = []
        parts = []
        for key, value in zip(type_def.keys, type_def.values):
            if key == 'max_dynamic_paths':
                try:
                    self.max_dynamic_paths = int(value)
                    parts.append(f'{key} = {value}')
                    continue
                except ValueError:
                    pass
            if key == 'max_dynamic_types':
                try:
                    self.max_dynamic_types = int(value)
                    parts.append(f'{key} = {value}')
                    continue
                except ValueError:
                    pass
            if key == 'SKIP':
                if value.startswith('REGEXP'):
                    value = 'REGEXP ' + value[6:]
                else:
                    if not value.startswith("`"):
                        value = f'`{value}`'
                skips.append(value)
            else:
                key = unescape_identifier(key)
                typed_paths.append(key)
                typed_types.append(get_from_name(value))
                key = f'`{key}`'
            parts.append(f'{key} {value}')
        if typed_paths:
            self.typed_paths = typed_paths
            self.typed_types = typed_types
        if skips:
            self.skips = skips
        if parts:
            self._name_suffix = f'({", ".join(parts)})'

    @property
    def insert_name(self):
        if json_serialization_format == 0:
            return 'String'
        return super().insert_name

    def write_column_prefix(self, dest: bytearray):
        if json_serialization_format > 0:
            write_uint64(json_serialization_format, dest)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext) -> JSONState:
        serialize_version = source.read_uint64()
        if serialize_version == 0:
            source.read_leb128()  # max dynamic types, we ignore this value
        elif serialize_version != 2:
            raise DataError(f'Unrecognized json structure version: {serialize_version} column: `{ctx.column_name}`')
        dynamic_path_cnt = source.read_leb128()
        dynamic_paths = [source.read_leb128_str() for _ in range(dynamic_path_cnt)]
        typed_states = [typed.read_column_prefix(source, ctx) for typed in self.typed_types]
        dynamic_states = [read_dynamic_prefix(self, source, ctx) for _ in range(dynamic_path_cnt)]
        return JSONState(serialize_version, dynamic_paths, typed_states, dynamic_states)

    # pylint: disable=too-many-locals
    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: JSONState):
        typed_columns = [ch_type.read_column_data(source, num_rows, ctx, read_state)
                         for ch_type, read_state in zip(self.typed_types, read_state.typed_states)]
        dynamic_columns = [
            read_variant_column(source, num_rows, ctx, dynamic_state.variant_types, dynamic_state.variant_states)
            for dynamic_state in read_state.dynamic_states]
        SHARED_DATA_TYPE.read_column_data(source, num_rows, ctx, None)
        col = []
        for row_num in range(num_rows):
            top = {}
            for ix, field in enumerate(self.typed_paths):
                value = typed_columns[ix][row_num]
                item = top
                chain = _json_path_segments(field)
                for key in chain[:-1]:
                    child = item.get(key)
                    if child is None:
                        child = {}
                        item[key] = child
                    item = child
                item[chain[-1]] = value
            for ix, field in enumerate(read_state.dynamic_paths):
                value = dynamic_columns[ix][row_num]
                if value is None:
                    continue
                item = top
                chain = _json_path_segments(field)
                for key in chain[:-1]:
                    child = item.get(key)
                    if child is None:
                        child = {}
                        item[key] = child
                    item = child
                item[chain[-1]] = value
            col.append(top)
        if self.read_format(ctx) == 'string':
            return [any_to_json(v) for v in col]
        return col


# Note that this type is deprecated and should not be used, it included for temporary backward compatibility only
class Object(ClickHouseType):
    python_type = dict
    # Native is a Python type (primitive, dict, array), string is an actual JSON string
    valid_formats = 'string', 'native'
    _data_size = json_sample_size
    write_column_data = write_json

    def __init__(self, type_def):
        data_type = type_def.values[0].lower().replace(' ', '')
        if data_type not in ("'json'", "nullable('json')"):
            raise NotImplementedError('Only json or Nullable(json) Object type is currently supported')
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def write_column_prefix(self, dest: bytearray):
        dest.append(0x01)
