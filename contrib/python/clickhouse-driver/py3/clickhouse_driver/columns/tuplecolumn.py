
from .base import Column
from .util import get_inner_spec, get_inner_columns_with_types


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, names, nested_columns, **kwargs):
        self.names = names
        self.nested_columns = nested_columns
        client_settings = kwargs['context'].client_settings
        settings = kwargs['context'].settings
        self.namedtuple_as_json = (
            settings.get('allow_experimental_object_type', False) and
            client_settings.get('namedtuple_as_json', True)
        )

        super(TupleColumn, self).__init__(**kwargs)
        self.null_value = tuple(x.null_value for x in nested_columns)

    def write_data(self, items, buf):
        items = self.prepare_items(items)
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            x.write_data(list(items[i]), buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        rv = [x.read_data(n_items, buf) for x in self.nested_columns]
        rv = list(zip(*rv))

        if self.names[0] and self.namedtuple_as_json:
            return [dict(zip(self.names, x)) for x in rv]
        else:
            return rv

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)

    def read_state_prefix(self, buf):
        super(TupleColumn, self).read_state_prefix(buf)

        for x in self.nested_columns:
            x.read_state_prefix(buf)

    def write_state_prefix(self, buf):
        super(TupleColumn, self).write_state_prefix(buf)

        for x in self.nested_columns:
            x.write_state_prefix(buf)


def create_tuple_column(spec, column_by_spec_getter, column_options):
    inner_spec = get_inner_spec('Tuple', spec)
    columns_with_types = get_inner_columns_with_types(inner_spec)
    names, types = zip(*columns_with_types)

    return TupleColumn(names, [column_by_spec_getter(x) for x in types],
                       **column_options)
