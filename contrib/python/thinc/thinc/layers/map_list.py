from typing import Callable, List, Optional, Tuple, TypeVar

from ..model import Model

InT = TypeVar("InT")
OutT = TypeVar("OutT")


def map_list(layer: Model[InT, OutT]) -> Model[List[InT], List[OutT]]:
    """Create a model that maps a child layer across list inputs."""
    return Model("map_list", forward, layers=[layer], init=init)


def forward(
    model: Model[List[InT], List[OutT]], Xs: List[InT], is_train: bool
) -> Tuple[List[OutT], Callable[[List[OutT]], List[InT]]]:
    layer = model.layers[0]
    Ys = []
    callbacks = []
    for X in Xs:
        Y, get_dX = layer(X, is_train)
        Ys.append(Y)
        callbacks.append(get_dX)

    def backprop_map_list(dYs: List[OutT]) -> List[InT]:
        return [callback(dY) for callback, dY in zip(callbacks, dYs)]

    return Ys, backprop_map_list


def init(
    model: Model[List[InT], List[OutT]],
    X: Optional[List[InT]] = None,
    Y: Optional[List[OutT]] = None,
) -> None:
    model.layers[0].initialize(X=X[0] if X else None, Y=Y[0] if Y else None)
