

def create_nullable_column(spec, column_by_spec_getter):
    inner = spec[9:-1]
    nested = column_by_spec_getter(inner)
    nested.nullable = True
    return nested
