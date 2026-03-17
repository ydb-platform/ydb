import typing as tp

from ....lib.std import (
    builtin_map,
    itertools_starmap,
)

# ---

def map_items[Item, NewItem](
    iterable: tp.Iterable[Item],
    func: tp.Callable[[Item], NewItem],
) -> tp.Iterator[NewItem]:
    return builtin_map(func, iterable)


def map_packed_items[*Values, NewItem](
    iterable: tp.Iterable[tp.Tuple[*Values]],
    func: tp.Callable[[*Values], NewItem],
) -> tp.Iterator[NewItem]:
    return itertools_starmap(func, iterable)
