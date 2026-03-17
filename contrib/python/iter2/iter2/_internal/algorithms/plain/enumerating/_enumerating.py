import typing as tp

from ....lib.std import builtin_enumerate


# ---

def enumerate_items[Item](
    iterable: tp.Iterable[Item],
    *,
    count_from: int = 0,
) -> tp.Iterator[tp.Tuple[int, Item]]:
    return builtin_enumerate(iterable, count_from)


def enumerate_packed_items[*Values](
    iterable: tp.Iterable[tp.Tuple[*Values]],
    *,
    count_from: int = 0,
) -> tp.Iterator[tp.Tuple[int, *Values]]:
    return (
        (idx, *tpl)
        for idx, tpl in builtin_enumerate(iterable, count_from)
    )
