import typing as tp

from ....lib.std import (
    itertools_islice,
    collections_deque,
)


# ---

def collect_first_items[Item](
    iterable: tp.Iterable[Item],
    *,
    count: int,
) -> tp.Tuple[Item, ...]:
    return tuple(itertools_islice(iterable, count))


def collect_last_items[Item](
    iterable: tp.Iterable[Item],
    *,
    count: int,
) -> tp.Tuple[Item, ...]:
    return tuple(collections_deque(iterable, maxlen=count))
