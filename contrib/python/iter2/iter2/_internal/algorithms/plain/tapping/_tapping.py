import typing as tp

from ....lib.std import (
    itertools_islice,
    functools_partial,
    contextlib_suppress,
)


# ---

def map_tap_items[Item](
    iterable: tp.Iterable[Item],
    action: tp.Callable[[Item], tp.Any],
) -> tp.Iterator[Item]:
    for item in iterable:
        action(item)
        yield item


def map_tap_packed_items[*Values](
    iterable: tp.Iterable[tp.Tuple[*Values]],
    action: tp.Callable[[*Values], tp.Any],
) -> tp.Iterator[tp.Tuple[*Values]]:
    for item in iterable:
        action(*item)
        yield item


def map_tap_items_periodically[Item](
    iterable: tp.Iterable[Item],
    *,
    step_size: int = 1,
    action: tp.Callable[[Item], tp.Any],
) -> tp.Iterator[Item]:
    it = iter(iterable)
    pass_rest = functools_partial(
        itertools_islice,
        it,
        step_size - 1,  # pass_size
    )
    with contextlib_suppress(StopIteration):
        while True:
            item = next(it)
            action(item)
            yield item
            yield from pass_rest()


def map_tap_packed_items_periodically[*Values](
    iterable: tp.Iterable[tp.Tuple[*Values]],
    *,
    step_size: int = 1,
    action: tp.Callable[[*Values], tp.Any],
) -> tp.Iterator[tp.Tuple[*Values]]:
    it = iter(iterable)
    pass_rest = functools_partial(
        itertools_islice,
        it,
        step_size - 1,  # pass_size
    )
    with contextlib_suppress(StopIteration):
        while True:
            item = next(it)
            action(*item)
            yield item
            yield from pass_rest()
