from typing import Callable, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats2d, Ragged
from ..util import ArrayInfo

InT = Ragged
OutT = Floats2d


@registry.layers("reduce_first.v1")
def reduce_first() -> Model[InT, OutT]:
    """Reduce ragged-formatted sequences to their first element."""
    return Model("reduce_first", forward)


def forward(
    model: Model[InT, OutT], Xr: InT, is_train: bool
) -> Tuple[OutT, Callable[[OutT], InT]]:
    Y, starts_ends = model.ops.reduce_first(cast(Floats2d, Xr.data), Xr.lengths)

    array_info = ArrayInfo.from_array(Y)

    def backprop(dY: OutT) -> InT:
        array_info.check_consistency(dY)
        dX = model.ops.backprop_reduce_first(dY, starts_ends)
        return Ragged(dX, Xr.lengths)

    return Y, backprop
