import array
import logging
from typing import Sequence, Collection, Any

from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.json_impl import any_to_json
from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.common import must_swap, first_value
from clickhouse_connect.datatypes.registry import get_from_name

logger = logging.getLogger(__name__)


class Array(ClickHouseType):
    __slots__ = ('element_type', '_insert_name')
    python_type = list

    @property
    def insert_name(self):
        return self._insert_name

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type = get_from_name(type_def.values[0])
        self._name_suffix = f'({self.element_type.name})'
        self._insert_name = f'Array({self.element_type.insert_name})'

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return self.element_type.read_column_prefix(source, ctx)

    def _data_size(self, sample: Sequence) -> int:
        if len(sample) == 0:
            return 8
        total = 0
        for x in sample:
            total += self.element_type.data_size(x)
        return total // len(sample) + 8

    # pylint: disable=too-many-locals
    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any):
        final_type = self.element_type
        depth = 1
        while isinstance(final_type, Array):
            depth += 1
            final_type = final_type.element_type
        level_size = num_rows
        offset_sizes = []
        for _ in range(depth):
            level_offsets = source.read_array('Q', level_size)
            offset_sizes.append(level_offsets)
            level_size = level_offsets[-1] if level_offsets else 0
        if level_size:
            all_values = final_type.read_column_data(source, level_size, ctx, read_state)
        else:
            all_values = []
        column = all_values if isinstance(all_values, list) else list(all_values)
        for offset_range in reversed(offset_sizes):
            data = []
            last = 0
            for x in offset_range:
                data.append(column[last: x])
                last = x
            column = data
        return column

    def write_column_prefix(self, dest: bytearray):
        self.element_type.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        final_type = self.element_type
        depth = 1
        while isinstance(final_type, Array):
            depth += 1
            final_type = final_type.element_type
        for _ in range(depth):
            total = 0
            data = []
            offsets = array.array('Q')
            for x in column:
                total += len(x)
                offsets.append(total)
                data.extend(x)
            if must_swap:
                offsets.byteswap()
            dest += offsets.tobytes()
            column = data
        final_type.write_column_data(column, dest, ctx)


class Tuple(ClickHouseType):
    _slots = 'element_names', 'element_types', '_insert_name'
    python_type = tuple
    valid_formats = 'tuple', 'dict', 'json', 'native'  # native is 'tuple' for unnamed tuples, and dict for named tuples

    @property
    def insert_name(self):
        return self._insert_name

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_names = type_def.keys
        self.element_types = [get_from_name(name) for name in type_def.values]
        if self.element_names:
            self._name_suffix = f"({', '.join(quote_identifier(k) + ' ' + str(v) for k, v in zip(type_def.keys, type_def.values))})"
        else:
            self._name_suffix = type_def.arg_str
        if self.element_names:
            self._insert_name = \
                f"Tuple({', '.join(quote_identifier(k) + ' ' + v.insert_name for k, v in zip(type_def.keys, self.element_types))})"
        else:
            self._insert_name = f"Tuple({', '.join(v.insert_name for v in self.element_types)})"

    def _data_size(self, sample: Collection) -> int:
        if len(sample) == 0:
            return 0
        elem_size = 0
        is_dict = self.element_names and isinstance(first_value(list(sample), self.nullable), dict)
        for ix, e_type in enumerate(self.element_types):
            if e_type.byte_size > 0:
                elem_size += e_type.byte_size
            elif is_dict:
                elem_size += e_type.data_size([x.get(self.element_names[ix], None) for x in sample])
            else:
                elem_size += e_type.data_size([x[ix] for x in sample])
        return elem_size

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return [e_type.read_column_prefix(source, ctx) for e_type in self.element_types]

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any):
        columns = []
        e_names = self.element_names
        for ix, e_type in enumerate(self.element_types):
            column = e_type.read_column_data(source, num_rows, ctx, read_state[ix])
            columns.append(column)
        if e_names and self.read_format(ctx) != 'tuple':
            dicts = [{} for _ in range(num_rows)]
            for ix, x in enumerate(dicts):
                for y, key in enumerate(e_names):
                    x[key] = columns[y][ix]
            if self.read_format(ctx) == 'json':
                to_json = any_to_json
                return [to_json(x) for x in dicts]
            return dicts
        return tuple(zip(*columns))

    def write_column_prefix(self, dest: bytearray):
        for e_type in self.element_types:
            e_type.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        if self.element_names and isinstance(first_value(column, self.nullable), dict):
            columns = self.convert_dict_insert(column)
        else:
            columns = list(zip(*column))
        for e_type, elem_column in zip(self.element_types, columns):
            e_type.write_column_data(elem_column, dest, ctx)

    def convert_dict_insert(self, column: Sequence) -> Sequence:
        names = self.element_names
        col = [[] for _ in names]
        for x in column:
            for ix, name in enumerate(names):
                col[ix].append(x.get(name))
        return col


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type', '_insert_name'
    python_type = dict

    @property
    def insert_name(self):
        return self._insert_name

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.key_type = get_from_name(type_def.values[0])
        self.value_type = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str
        self._insert_name = f'Map({self.key_type.insert_name}, {self.value_type.insert_name})'

    def _data_size(self, sample: Collection) -> int:
        total = 0
        if len(sample) == 0:
            return 0
        for x in sample:
            total += self.key_type.data_size(x.keys())
            total += self.value_type.data_size(x.values())
        return total // len(sample)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        key_state = self.key_type.read_column_prefix(source, ctx)
        value_state = self.value_type.read_column_prefix(source, ctx)
        return key_state, value_state

    # pylint: disable=too-many-locals
    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any):
        offsets = source.read_array('Q', num_rows)
        total_rows = 0 if len(offsets) == 0 else offsets[-1]
        keys = self.key_type.read_column_data(source, total_rows, ctx, read_state[0])
        values = self.value_type.read_column_data(source, total_rows, ctx, read_state[1])
        all_pairs = tuple(zip(keys, values))
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            app(dict(all_pairs[last: offset]))
            last = offset
        return column

    def write_column_prefix(self, dest: bytearray):
        self.key_type.write_column_prefix(dest)
        self.value_type.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        offsets = array.array('Q')
        keys = []
        values = []
        total = 0
        for v in column:
            total += len(v)
            offsets.append(total)
            keys.extend(v.keys())
            values.extend(v.values())
        if must_swap:
            offsets.byteswap()
        dest += offsets.tobytes()
        self.key_type.write_column_data(keys, dest, ctx)
        self.value_type.write_column_data(values, dest, ctx)


class Nested(ClickHouseType):
    __slots__ = 'tuple_array', 'element_names', 'element_types'
    python_type = Sequence[dict]

    def __init__(self, type_def):
        super().__init__(type_def)
        self.element_names = type_def.keys
        self.tuple_array = get_from_name(f"Array(Tuple({','.join(type_def.values)}))")
        self.element_types = self.tuple_array.element_type.element_types
        cols = [f'{x[0]} {x[1].name}' for x in zip(type_def.keys, self.element_types)]
        self._name_suffix = f"({', '.join(cols)})"

    def _data_size(self, sample: Collection) -> int:
        keys = self.element_names
        array_sample = [[tuple(sub_row[key] for key in keys) for sub_row in row] for row in sample]
        return self.tuple_array.data_size(array_sample)

    def read_column_prefix(self, source: ByteSource, ctx: QueryContext):
        return self.tuple_array.read_column_prefix(source, ctx)

    def read_column_data(self, source: ByteSource, num_rows: int, ctx: QueryContext, read_state: Any):
        keys = self.element_names
        data = self.tuple_array.read_column_data(source, num_rows, ctx, read_state)
        return [[dict(zip(keys, x)) for x in row] for row in data]

    def write_column_prefix(self, dest: bytearray):
        self.tuple_array.write_column_prefix(dest)

    def write_column_data(self, column: Sequence, dest: bytearray, ctx: InsertContext):
        keys = self.element_names
        data = [[tuple(sub_row[key] for key in keys) for sub_row in row] for row in column]
        self.tuple_array.write_column_data(data, dest, ctx)
