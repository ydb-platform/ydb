import typing as tp
from ....algorithms.plain import generating

from ._make_iterator2_builder import (
    make_iterator2_from_iterator_builder as It2,
    make_iterator2_from_iterable_builder as It2Sized,
)


# ---

@It2Sized
def from_args[T](*args: T) -> tp.Tuple[T, ...]:
    return args


count_from = It2(generating.count_from)

cycle_through_items = It2(generating.cycle_through_items)
yield_item_repeatedly = It2(generating.yield_item_repeatedly)
repeatedly_call_forever = It2(generating.repeatedly_call_forever)

