
from .base import Column


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)

    def write_data(self, items, buf):
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            x.write_data(list(items[i]), buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        rv = [x.read_data(n_items, buf) for x in self.nested_columns]
        return list(zip(*rv))

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)


def create_tuple_column(spec, column_by_spec_getter):
    brackets = 0
    column_begin = 0

    inner_spec = get_inner_spec(spec)
    nested_columns = []
    for i, x in enumerate(inner_spec + ','):
        if x == ',':
            if brackets == 0:
                nested_columns.append(inner_spec[column_begin:i])
                column_begin = i + 1
        elif x == '(':
            brackets += 1
        elif x == ')':
            brackets -= 1
        elif x == ' ':
            if brackets == 0:
                column_begin = i + 1

    return TupleColumn([column_by_spec_getter(x) for x in nested_columns])


def get_inner_spec(spec):
    brackets = 1
    offset = len('Tuple(')
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if brackets == 0:
            break

        if ch == '(':
            brackets += 1

        elif ch == ')':
            brackets -= 1

    return spec[offset:i]
