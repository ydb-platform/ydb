from typing import Callable, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats2d, Ragged
from ..util import ArrayInfo

InT = Ragged
OutT = Floats2d


@registry.layers("reduce_sum.v1")
def reduce_sum() -> Model[InT, OutT]:
    return Model("reduce_sum", forward)


def forward(model: Model[InT, OutT], Xr: InT, is_train: bool) -> Tuple[OutT, Callable]:
    Y = model.ops.reduce_sum(cast(Floats2d, Xr.data), Xr.lengths)
    lengths = Xr.lengths
    array_info = ArrayInfo.from_array(Y)

    def backprop(dY: OutT) -> InT:
        array_info.check_consistency(dY)
        return Ragged(model.ops.backprop_reduce_sum(dY, lengths), lengths)

    return Y, backprop
