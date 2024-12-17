from typing import List, Sequence, Collection

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.common import unescape_identifier, first_value
from clickhouse_connect.driver.ctypes import data_conv
from clickhouse_connect.driver.errors import handle_error
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.json_impl import any_to_json

SHARED_DATA_TYPE: ClickHouseType
STRING_DATA_TYPE: ClickHouseType

class Variant(ClickHouseType):
    _slots = 'element_types'
    python_type = object

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_types:List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self._name_suffix = f"({', '.join(ch_type.name for ch_type in self.element_types)})"

    @property
    def insert_name(self):
        return 'String'

    def read_column_prefix(self, source: ByteSource, ctx:QueryContext):
        if source.read_uint64() != 0:
            raise DataError(f'Unexpected discriminator format in Variant column {ctx.column_name}')

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext) -> Sequence:
        return read_variant_column(self.element_types, source, num_rows, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        write_str_values(self, column, dest, ctx)


def read_variant_column(variant_types: List[ClickHouseType], source: ByteSource, num_rows:int, ctx: QueryContext) -> Sequence:
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
            sub_columns[ix] = variant_types[ix].read_column_data(source, disc_rows[ix], ctx)
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


class Dynamic(ClickHouseType):
    python_type = object

    @property
    def insert_name(self):
        return 'String'

    def __init__(self, type_def:TypeDef):
        super().__init__(type_def)
        if type_def.keys and type_def.keys[0] == 'max_types':
            self._name_suffix = f'(max_types={type_def.values[0]})'

    def read_column(self, source: ByteSource, num_rows:int, ctx:QueryContext):
        variant_types = read_dynamic_prefix(source)
        return read_variant_column(variant_types, source, num_rows, ctx)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        write_str_values(self, column, dest, ctx)


def read_dynamic_prefix(source: ByteSource) -> List[ClickHouseType]:
    if source.read_uint64() != 1:  # dynamic structure serialization version, currently only 1 is recognized
        raise DataError('Unrecognized dynamic structure version')
    source.read_leb128()  # max dynamic types, we ignore this value
    num_variants = source.read_leb128()
    variant_types = [get_from_name(source.read_leb128_str()) for _ in range(num_variants)]
    variant_types.append(STRING_DATA_TYPE)
    if source.read_uint64() != 0: # discriminator format, currently only 0 is recognized
        raise DataError('Unexpected discriminator format in Variant column prefix')
    return variant_types


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


def write_json(ch_type:ClickHouseType, column: Sequence, dest: bytearray, ctx: InsertContext):
    first = first_value(column, ch_type.nullable)
    write_col = column
    encoding = ctx.encoding or ch_type.encoding
    if not isinstance(first, str) and ch_type.write_format(ctx) != 'string':
        to_json = any_to_json
        write_col = [to_json(v) for v in column]
        encoding = None
    handle_error(data_conv.write_str_col(write_col, ch_type.nullable, encoding, dest), ctx)


def write_str_values(ch_type:ClickHouseType, column: Sequence, dest: bytearray, ctx: InsertContext):
    encoding = ctx.encoding or ch_type.encoding
    col = [''] * len(column)
    for ix, v in enumerate(column):
        if v is None:
            col[ix] = 'NULL'
        else:
            col[ix] = str(v)
    handle_error(data_conv.write_str_col(col, False, encoding, dest), ctx)


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

    def __init__(self, type_def:TypeDef):
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
        return 'String'

    # pylint: disable=too-many-locals
    def read_column(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if source.read_uint64() != 0: # object serialization version, currently only 0 is recognized
            raise DataError(f'unrecognized object serialization version, column `{ctx.column_name}`')
        source.read_leb128() # the max number of dynamic paths.  Used to preallocate storage in ClickHouse; we ignore it
        dynamic_path_cnt = source.read_leb128()
        dynamic_paths = [source.read_leb128_str() for _ in range(dynamic_path_cnt)]
        for typed in self.typed_types:
            typed.read_column_prefix(source, ctx)
        dynamic_variants = [read_dynamic_prefix(source) for _ in range(dynamic_path_cnt)]
        # C++ prefix read ends here

        typed_columns = [ch_type.read_column_data(source, num_rows, ctx) for ch_type in self.typed_types]
        dynamic_columns = [read_variant_column(dynamic_variants[ix], source, num_rows, ctx) for ix in range(dynamic_path_cnt)]
        SHARED_DATA_TYPE.read_column_data(source, num_rows, ctx)
        col = []
        for row_num in range(num_rows):
            top = {}
            for ix, field in enumerate(self.typed_paths):
                value = typed_columns[ix][row_num]
                item = top
                chain = field.split('.')
                for key in chain[:-1]:
                    child = item.get(key)
                    if child is None:
                        child = {}
                        item[key] = child
                    item = child
                item[chain[-1]] = value
            for ix, field in enumerate(dynamic_paths):
                value = dynamic_columns[ix][row_num]
                if value is None:
                    continue
                item = top
                chain = field.split('.')
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
