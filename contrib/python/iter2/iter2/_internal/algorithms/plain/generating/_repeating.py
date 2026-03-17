import typing as tp

from ....lib.std import (
    itertools_chain,
    itertools_cycle,
    itertools_tee,
)
from ....lib.undefined import UNDEFINED
from ....lib.functions import Thunk


# ---

itertools_chain_from_iterable = itertools_chain.from_iterable


def cycle_through_items[Item](
    iterable: tp.Iterable[Item],  # TODO: may be `Iterable[T] & Sized`
    *,
    number_of_times: tp.Optional[int] = None,
) -> tp.Iterator[Item]:
    if number_of_times is None:
        return itertools_cycle(iterable)
    else:
        return itertools_chain_from_iterable(
            itertools_tee(
                iterable,
                number_of_times,
            )
        )


def yield_item_repeatedly[Item](
    item: Item,
    *,
    number_of_times: tp.Optional[int] = None,
) -> tp.Iterator[Item]:
    return cycle_through_items((item,), number_of_times=number_of_times)


def repeatedly_call_forever[Item](fn: Thunk[Item]) -> tp.Iterator[Item]:
    return iter(fn, UNDEFINED)
