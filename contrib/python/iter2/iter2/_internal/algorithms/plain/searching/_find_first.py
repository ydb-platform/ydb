import typing as tp

from ....lib.std import builtin_filter
from ....lib.undefined import (
    UNDEFINED,
    undefined_to_option,
)
from ....lib.functions import Predicate
from ....lib.boxed_values import Option2


# ---

def find_first_occurrence[Item](
    iterable: tp.Iterable[Item],
    predicate: Predicate[Item],
) -> Option2[Item]:
    return undefined_to_option(
        next(
            builtin_filter(predicate, iterable),
            UNDEFINED,
        )
    )
