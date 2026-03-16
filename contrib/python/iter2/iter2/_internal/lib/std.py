# flake8: noqa

builtin_map = map
builtin_filter = filter

builtin_enumerate = enumerate

builtin_zip = zip

builtin_min = min
builtin_max = max

builtin_sum = sum


# ---

from itertools import (
    starmap as itertools_starmap,
    filterfalse as itertools_filter_false,
    takewhile as itertools_takewhile,
    dropwhile as itertools_dropwhile,

    chain as itertools_chain,
    cycle as itertools_cycle,
    zip_longest as itertools_zip_longest,

    groupby as itertools_groupby,

    islice as itertools_islice,
    batched as itertools_batched,

    count as itertools_count,
    accumulate as itertools_accumulate,

    tee as itertools_tee,

    product as itertools_product,
)

itertools_chain_from_iterable = itertools_chain.from_iterable

from functools import (
    partial as functools_partial,
    reduce as functools_reduce,
)

from collections import (
    deque as collections_deque,
)

from operator import (
    itemgetter as operator_itemgetter,
    call as operator_call,
)

from contextlib import suppress as contextlib_suppress
