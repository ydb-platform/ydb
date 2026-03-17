from ....lib.std import (
    builtin_zip,
    itertools_starmap,
    itertools_chain_from_iterable,
    itertools_cycle,
    operator_call,
)

from ..folding import consume_iterator


# ---

def unzip_into_lists(iterable, *, arity_hint):
    result_lists = tuple([] for _ in range(arity_hint))
    cycled_appends = itertools_cycle(tuple(
        xs.append for xs in result_lists
    ))
    flattened_iterable = itertools_chain_from_iterable(iterable)

    consume_iterator(
        itertools_starmap(
            operator_call,
            builtin_zip(cycled_appends, flattened_iterable)
        )
    )

    return result_lists
