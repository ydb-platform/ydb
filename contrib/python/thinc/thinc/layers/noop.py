from typing import Callable, Tuple, TypeVar

from ..config import registry
from ..model import Model

InOutT = TypeVar("InOutT")


@registry.layers("noop.v1")
def noop(*layers: Model) -> Model[InOutT, InOutT]:
    """Transform a sequences of layers into a null operation."""
    return Model("noop", forward, layers=layers)


def forward(
    model: Model[InOutT, InOutT], X: InOutT, is_train: bool
) -> Tuple[InOutT, Callable]:
    def backprop(dY: InOutT) -> InOutT:
        return dY

    return X, backprop
