

def assert_type(item, types):
    if not isinstance(item, types):
        if not isinstance(types, tuple):
            types = [types]
        raise TypeError('expected {types}, got {type}'.format(
            types=' or '.join(_.__name__ for _ in types),
            type=type(item).__name__
        ))


def assert_subclass(item, Class):
    if not issubclass(item, Class):
        raise TypeError('expected {expected}, got {got}'.format(
            expected=Class.__name__,
            got=item.__name__
        ))


def assert_equals(item, value):
    if item != value:
        raise ValueError('expected {value!r}, got {item!r}'.format(
            item=item,
            value=value
        ))


def assert_greater_equals(a, b):
    if a < b:
        raise ValueError('expected a>=b, got a={a!r}, b={b!r}'.format(
            a=a,
            b=b
        ))


def assert_not_empty(item):
    if len(item) == 0:
        raise ValueError('expected not empty')
