from struct import Struct, error as struct_error

from . import exceptions
from ..varint import read_varint


class CommonSerialization(object):
    def __init__(self, column):
        self.column = column
        super(CommonSerialization, self).__init__()

    def read_sparse(self, n_items, buf):
        return n_items

    def apply_sparse(self, items):
        return items


class SparseSerialization(CommonSerialization):

    def __init__(self, *args, **kwargs):
        self.sparse_indexes = []
        self.items_total = None
        super(SparseSerialization, self).__init__(*args, **kwargs)

    def read_sparse(self, n_items, buf):
        sparse_indexes = []
        items_total = 0
        non_default_items = 0

        END_OF_GRANULE_FLAG = 1 << 62
        end_of_granule = False

        while not end_of_granule:
            group_size = read_varint(buf)
            end_of_granule = group_size & END_OF_GRANULE_FLAG
            group_size &= ~END_OF_GRANULE_FLAG

            items_total += group_size + 1
            if not end_of_granule:
                non_default_items += 1
                sparse_indexes.append(items_total)

        self.sparse_indexes = sparse_indexes
        self.items_total = items_total

        return non_default_items

    def apply_sparse(self, items):
        default = self.column.null_value
        if self.column.after_read_items:
            default = self.column.after_read_items([default])[0]

        rv = [default] * (self.items_total - 1)
        for item_number, i in enumerate(self.sparse_indexes):
            rv[i - 1] = items[item_number]

        return rv


class Column(object):
    ch_type = None
    py_types = None

    check_item = None
    after_read_items = None
    before_write_items = None

    types_check_enabled = False

    null_value = 0

    def __init__(self, types_check=False, has_custom_serialization=False,
                 **kwargs):
        self.nullable = False
        self.types_check_enabled = types_check
        self.has_custom_serialization = has_custom_serialization
        self.serialization = CommonSerialization(self)
        self.input_null_as_default = False

        self.context = kwargs['context']
        self.input_null_as_default = self.context.client_settings \
            .get('input_format_null_as_default', False)

        super(Column, self).__init__()

    def make_null_struct(self, n_items):
        return Struct('<{}B'.format(n_items))

    def _read_nulls_map(self, n_items, buf):
        s = self.make_null_struct(n_items)
        return s.unpack(buf.read(s.size))

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        items = [x is None for x in items]
        buf.write(s.pack(*items))

    def check_item_type(self, value):
        if not isinstance(value, self.py_types):
            raise exceptions.ColumnTypeMismatchException(value)

    def prepare_items(self, items):
        nullable = self.nullable
        null_value = self.null_value
        null_as_default = self.input_null_as_default

        check_item = self.check_item
        if self.types_check_enabled:
            check_item_type = self.check_item_type
        else:
            check_item_type = False

        if (not (self.nullable or null_as_default) and not check_item_type and
                not check_item and not self.before_write_items):
            return items

        nulls_map = [False] * len(items) if self.nullable else None
        for i, x in enumerate(items):
            if x is None:
                if nullable:
                    nulls_map[i] = True
                    x = null_value
                elif null_as_default:
                    x = null_value

            else:
                if check_item_type:
                    check_item_type(x)

                if check_item:
                    check_item(x)

            items[i] = x

        if self.before_write_items:
            self.before_write_items(items, nulls_map=nulls_map)

        return items

    def write_data(self, items, buf):
        if self.nullable:
            self._write_nulls_map(items, buf)

        self._write_data(items, buf)

    def _write_data(self, items, buf):
        prepared = self.prepare_items(items)
        self.write_items(prepared, buf)

    def write_items(self, items, buf):
        raise NotImplementedError

    def read_data(self, n_items, buf):
        n_items = self.serialization.read_sparse(n_items, buf)

        if self.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)
        else:
            nulls_map = None

        items = self._read_data(n_items, buf, nulls_map=nulls_map)
        return self.serialization.apply_sparse(items)

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            return tuple(
                (None if is_null else items[i])
                for i, is_null in enumerate(nulls_map)
            )
        return items

    def read_items(self, n_items, buf):
        raise NotImplementedError

    def read_state_prefix(self, buf):
        if self.has_custom_serialization:
            use_custom_serialization = read_varint(buf)
            if use_custom_serialization:
                self.serialization = SparseSerialization(self)

    def write_state_prefix(self, buf):
        pass


class FormatColumn(Column):
    """
    Uses struct.pack for bulk items writing.
    """

    format = None

    def make_struct(self, n_items):
        return Struct('<{}{}'.format(n_items, self.format))

    def write_items(self, items, buf):
        s = self.make_struct(len(items))
        try:
            buf.write(s.pack(*items))

        except struct_error as e:
            raise exceptions.StructPackException(e)

    def read_items(self, n_items, buf):
        s = self.make_struct(n_items)
        return s.unpack(buf.read(s.size))


# How to write new column?
# - Check ClickHouse documentation for column
# - Wireshark and tcpdump are your friends.
# - Use `clickhouse-client --compression 0` to see what's going on data
#   transmission.
# - Check for similar existing columns and tests.
# - Use `FormatColumn` for columns that use "simple" types under the hood.
# - Some columns have before_write and after_read hooks.
#   Use them to convert items in column into "simple" types.
