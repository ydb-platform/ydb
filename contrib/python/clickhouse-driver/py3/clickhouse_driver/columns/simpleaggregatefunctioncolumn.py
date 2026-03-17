

def create_simple_aggregate_function_column(spec, column_by_spec_getter):
    # SimpleAggregateFunction(Func, Type) -> Type
    inner = spec[24:-1].split(',', 1)[1].strip()
    nested = column_by_spec_getter(inner)
    return nested
