from .base import Column
from .stringcolumn import String
from ..reader import read_binary_uint8, read_binary_str
from ..util.compat import json
from ..writer import write_binary_uint8


class JsonColumn(Column):
    py_types = (dict, )

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super(JsonColumn, self).__init__(**kwargs)

    def write_state_prefix(self, buf):
        # Read in binary format.
        # Write in text format.
        write_binary_uint8(1, buf)

    def read_items(self, n_items, buf):
        read_binary_uint8(buf)
        spec = read_binary_str(buf)
        col = self.column_by_spec_getter(spec)
        col.read_state_prefix(buf)
        return col.read_data(n_items, buf)

    def write_items(self, items, buf):
        items = [x if isinstance(x, str) else json.dumps(x) for x in items]
        self.string_column.write_items(items, buf)


def create_json_column(spec, column_by_spec_getter, column_options):
    return JsonColumn(column_by_spec_getter, **column_options)
