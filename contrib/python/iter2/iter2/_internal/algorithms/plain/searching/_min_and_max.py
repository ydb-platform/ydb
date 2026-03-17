from ....lib.std import (
    builtin_min,
    builtin_max,
)
from ....lib.undefined import (
    UNDEFINED,
    undefined_to_option,
)


# ---

def find_min_value(iterable, *, key=None):
    return undefined_to_option(
        builtin_min(iterable, default=UNDEFINED, key=key)
    )


def find_max_value(iterable, *, key=None):
    return undefined_to_option(
        builtin_max(iterable, default=UNDEFINED, key=key)
    )
