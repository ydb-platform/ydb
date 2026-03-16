import typing as tp

from ....lib.std import (
    builtin_map,
    itertools_groupby,
)
from ....lib.operators import (
    first_item,
    second_item,
)


# ---

def group_consecutive_values_with_same_computable_key[Key, Value](
    iterable: tp.Iterable[Value],
    *,
    key_fn: tp.Callable[[Value], Key],
) -> tp.Iterator[tp.Tuple[Key, tp.Iterator[Value]]]:
    return itertools_groupby(iterable, key=key_fn)


# ---

def group_consecutive_values_by_key[Key, Value](
    iterable: tp.Iterable[tp.Tuple[Key, Value]],
) -> tp.Iterator[tp.Tuple[Key, tp.Iterator[Value]]]:
    return (
        (key, builtin_map(second_item, entries))
        for key, entries in itertools_groupby(iterable, key=first_item)
    )
