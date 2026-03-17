
from .arraycolumn import create_array_column
from .util import get_inner_spec


def create_nested_column(spec, column_by_spec_getter, column_options):
    return create_array_column(
        'Array(Tuple({}))'.format(get_inner_spec('Nested', spec)),
        column_by_spec_getter, column_options
    )
