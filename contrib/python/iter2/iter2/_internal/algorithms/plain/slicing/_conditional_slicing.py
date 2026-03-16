import typing as tp

from ....lib.std import (
    itertools_takewhile,
    itertools_dropwhile,
)
from ....lib.functions import Predicate


# ---

def drop_items_while_true_and_iterate_rest[Item](
    iterable: tp.Iterable[Item],
    predicate: Predicate[Item],
) -> tp.Iterator[Item]:
    return itertools_dropwhile(predicate, iterable)


# ---

def iterate_through_items_while_true_and_drop_first_false[Item](
    iterable: tp.Iterable[Item],
    predicate: Predicate[Item],
) -> tp.Iterator[Item]:
    return itertools_takewhile(predicate, iterable)
