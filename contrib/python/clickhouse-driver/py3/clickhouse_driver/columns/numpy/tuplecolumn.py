import numpy as np

from .base import NumpyColumn
from ..util import get_inner_spec, get_inner_columns


class TupleColumn(NumpyColumn):
    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)

    def write_data(self, items, buf):
        names = items.dtype.names
        for i, (x, name) in enumerate(zip(self.nested_columns, names)):
            x.write_data(items[name], buf)

    def write_items(self, items, buf):
        return self.write_data(items, buf)

    def read_data(self, n_items, buf):
        data = [x.read_data(n_items, buf) for x in self.nested_columns]
        dtype = [('f{}'.format(i), x.dtype) for i, x in enumerate(data)]
        rv = np.empty(n_items, dtype=dtype)
        for i, x in enumerate(data):
            rv['f{}'.format(i)] = x
        return rv

    def read_items(self, n_items, buf):
        return self.read_data(n_items, buf)


def create_tuple_column(spec, column_by_spec_getter, column_options):
    inner_spec = get_inner_spec('Tuple', spec)
    columns = get_inner_columns(inner_spec)

    return TupleColumn([column_by_spec_getter(x) for x in columns],
                       **column_options)
