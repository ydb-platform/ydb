import typing as tp

from ..undefined import UNDEFINED
from ..functions import Predicate


# ---

@tp.overload
def contains[Item](
    container: tp.Container[Item],
    item: Item,
) -> bool: ...


@tp.overload
def contains[Item](
    item: Item,
) -> Predicate[tp.Container[Item]]: ...


def contains(arg1, arg2=UNDEFINED):  # type: ignore
    if UNDEFINED.is_not(arg2):
        return arg2 in arg1
    else:
        return (lambda container: arg1 in container)
