from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import Expression
from django.db.models.lookups import Transform


def lookups_for_field(model_field):
    """Generate a list of all possible lookup expressions for a model field.

    Args:
        model_field: The model field to inspect.

    Returns:
        A list of lookups for the given ``model_field``.
    """
    lookups = []

    for expr, lookup in model_field.get_lookups().items():
        if issubclass(lookup, Transform):
            transform = lookup(Expression(model_field))
            lookups += [
                LOOKUP_SEP.join([expr, sub_expr]) for sub_expr
                in lookups_for_transform(transform)
            ]

        else:
            lookups.append(expr)

    return lookups


def lookups_for_transform(transform):
    """Generate a list of subsequent lookup expressions for a transform.

    Note:
    Infinite transform recursion is only prevented when the subsequent and passed in
    transforms are the same class. For example, the ``Unaccent`` transform from
    ``django.contrib.postgres``.
    There is no cycle detection across multiple transforms. For example, ``a__b__a__b``
    would continue to recurse. However, this is not currently a problem (no builtin
    transforms exhibit this behavior).

    Args:
        transform: The transform to inspect.

    Returns:
        A list of lookups for the given ``transform``.
    """
    lookups = []

    for expr, lookup in transform.output_field.get_lookups().items():
        if issubclass(lookup, Transform):

            # type match indicates recursion.
            if type(transform) == lookup:
                continue

            sub_transform = lookup(transform)
            lookups += [
                LOOKUP_SEP.join([expr, sub_expr]) for sub_expr
                in lookups_for_transform(sub_transform)
            ]

        else:
            lookups.append(expr)

    return lookups


def lookahead(iterable):
    it = iter(iterable)
    try:
        current = next(it)
    except StopIteration:
        return

    for value in it:
        yield current, True
        current = value
    yield current, False
