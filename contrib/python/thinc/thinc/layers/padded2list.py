from typing import Callable, Tuple, TypeVar, cast

from ..config import registry
from ..model import Model
from ..types import List2d, Padded

InT = Padded
OutT = TypeVar("OutT", bound=List2d)


@registry.layers("padded2list.v1")
def padded2list() -> Model[InT, OutT]:
    """Create a layer to convert a Padded input into a list of arrays."""
    return Model(f"padded2list", forward)


def forward(
    model: Model[InT, OutT], Xp: InT, is_train: bool
) -> Tuple[OutT, Callable[[OutT], InT]]:
    Ys = cast(OutT, model.ops.padded2list(Xp))

    def backprop(dYs: OutT) -> InT:
        dYp = model.ops.list2padded(dYs)
        assert isinstance(dYp, Padded)
        return dYp

    return Ys, backprop
