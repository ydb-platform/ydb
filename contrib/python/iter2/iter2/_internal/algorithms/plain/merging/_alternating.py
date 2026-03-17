from ....lib.std import (
    builtin_zip,
    itertools_chain_from_iterable,
    itertools_zip_longest,
)

from ..generating import cycle_through_items


# ---

def iterate_alternating(*iterables):
    MISSING_MARK = object()  # truly unique value
    return (
        item
        for item in itertools_chain_from_iterable(
            itertools_zip_longest(*iterables, fillvalue=MISSING_MARK)
        )
        if item is not MISSING_MARK
    )


def iterate_alternating_with_item(iterable, *, item):
    it = iter(iterable)
    try:
        yield next(it)
    except StopIteration:
        return

    yield from itertools_chain_from_iterable(
        builtin_zip(
            cycle_through_items((item,)),
            it,
        )
    )
