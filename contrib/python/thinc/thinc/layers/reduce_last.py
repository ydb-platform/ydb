from typing import Callable, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats2d, Ragged
from ..util import ArrayInfo

InT = Ragged
OutT = Floats2d


@registry.layers("reduce_last.v1")
def reduce_last() -> Model[InT, OutT]:
    """Reduce ragged-formatted sequences to their last element."""
    return Model("reduce_last", forward)


def forward(
    model: Model[InT, OutT], Xr: InT, is_train: bool
) -> Tuple[OutT, Callable[[OutT], InT]]:
    Y, lasts = model.ops.reduce_last(cast(Floats2d, Xr.data), Xr.lengths)
    array_info = ArrayInfo.from_array(Y)

    def backprop(dY: OutT) -> InT:
        array_info.check_consistency(dY)
        dX = model.ops.backprop_reduce_last(dY, lasts)
        return Ragged(dX, Xr.lengths)

    return Y, backprop
