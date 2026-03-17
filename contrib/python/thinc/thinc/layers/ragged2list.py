from typing import Callable, Tuple, TypeVar, cast

from ..config import registry
from ..model import Model
from ..types import ListXd, Ragged

InT = Ragged
OutT = TypeVar("OutT", bound=ListXd)


@registry.layers("ragged2list.v1")
def ragged2list() -> Model[InT, OutT]:
    """Transform sequences from a ragged format into lists."""
    return Model("ragged2list", forward)


def forward(model: Model[InT, OutT], Xr: InT, is_train: bool) -> Tuple[OutT, Callable]:
    lengths = Xr.lengths

    def backprop(dXs: OutT) -> InT:
        return Ragged(model.ops.flatten(dXs, pad=0), lengths)  # type:ignore[arg-type]
        # type ignore necessary for older versions of Mypy/Pydantic

    data = cast(OutT, model.ops.unflatten(Xr.dataXd, Xr.lengths))
    return data, backprop
