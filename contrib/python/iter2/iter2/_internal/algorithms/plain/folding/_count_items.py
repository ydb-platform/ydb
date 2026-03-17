import typing as tp

from ....lib.std import (
    builtin_zip,
    builtin_sum,
    itertools_count,
    collections_deque,
)


# ---

def count_items_via_deque_consuming(iterable: tp.Iterable) -> int:
    # 1. Init `counter` from 0 to infinity
    counter = itertools_count()
    # 2. Consume `iterable` with `counter` simultaneously
    collections_deque(
        builtin_zip(
            iterable,  # item
            counter,   # 0-based index
        ),
        maxlen=0,
    )
    # 3. Return 1-based index == count of items
    return next(counter)


def count_items_by_summing_ones(iterable: tp.Iterable) -> int:
    return builtin_sum(1 for _ in iterable)


# ---

count_items = count_items_via_deque_consuming
