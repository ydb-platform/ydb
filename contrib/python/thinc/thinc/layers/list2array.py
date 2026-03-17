from typing import Callable, List, Tuple, TypeVar

from ..backends import NumpyOps
from ..config import registry
from ..model import Model
from ..types import Array2d

NUMPY_OPS = NumpyOps()


OutT = TypeVar("OutT", bound=Array2d)
InT = List[OutT]


@registry.layers("list2array.v1")
def list2array() -> Model[InT, OutT]:
    """Transform sequences to ragged arrays if necessary and return the data
    from the ragged array. If sequences are already ragged, do nothing. A
    ragged array is a tuple (data, lengths), where data is the concatenated data.
    """
    return Model("list2array", forward)


def forward(model: Model[InT, OutT], Xs: InT, is_train: bool) -> Tuple[OutT, Callable]:
    lengths = NUMPY_OPS.asarray1i([len(x) for x in Xs])

    def backprop(dY: OutT) -> InT:
        return model.ops.unflatten(dY, lengths)

    return model.ops.flatten(Xs), backprop
