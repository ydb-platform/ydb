from typing import Any, Callable, Optional, Tuple

from ..config import registry
from ..model import Model

InT = Tuple[Any, ...]
OutT = Tuple[Any, ...]


@registry.layers("with_getitem.v1")
def with_getitem(idx: int, layer: Model) -> Model[InT, OutT]:
    """Transform data on the way into and out of a layer, by plucking an item
    from a tuple.
    """
    return Model(
        f"with_getitem({layer.name})",
        forward,
        init=init,
        layers=[layer],
        attrs={"idx": idx},
    )


def forward(
    model: Model[InT, OutT], items: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    idx = model.attrs["idx"]
    Y_i, backprop_item = model.layers[0](items[idx], is_train)

    def backprop(d_output: OutT) -> InT:
        dY_i = backprop_item(d_output[idx])
        return d_output[:idx] + (dY_i,) + d_output[idx + 1 :]

    return items[:idx] + (Y_i,) + items[idx + 1 :], backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    idx = model.attrs["idx"]
    X_i = X[idx] if X is not None else X
    Y_i = Y[idx] if Y is not None else Y
    model.layers[0].initialize(X=X_i, Y=Y_i)
