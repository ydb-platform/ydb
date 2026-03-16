import typing as tp

from ....lib.std import (
    builtin_filter,
    itertools_filter_false,
)
from ....lib.functions import Predicate
from ....lib.operators import is_instance_of


# ---

def filter_items[Item](
    iterable: tp.Iterable[Item],
    predicate: Predicate[Item],
    *,
    inverse: bool = False,
) -> tp.Iterator[Item]:
    if inverse:
        return itertools_filter_false(predicate, iterable)
    else:
        return builtin_filter(predicate, iterable)


def filter_none_items[Item](iterable: tp.Iterable[tp.Optional[Item]]) -> tp.Iterator[Item]:
    return (
        val
        for val in iterable
        if val is not None
    )


def filter_items_by_type_predicate[Item, DesiredType](
    iterable: tp.Iterable[Item],
    type_predicate: tp.Callable[[Item], tp.TypeGuard[DesiredType]],
) -> tp.Iterator[DesiredType]:
    return builtin_filter(
        type_predicate,
        iterable,
    )


def filter_items_by_type[Item, DesiredType](
    iterable: tp.Iterable[Item],
    type: tp.Type[DesiredType],
) -> tp.Iterator[DesiredType]:
    return builtin_filter(
        is_instance_of(type),
        iterable,
    )
