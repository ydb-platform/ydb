from typing import Any, Optional, Tuple, TypeVar

from ..config import registry
from ..model import Model

InT = TypeVar("InT")
OutT = Tuple


@registry.layers("tuplify.v1")
def tuplify(
    layer1: Model[InT, Any], layer2: Model[InT, Any], *layers
) -> Model[InT, Tuple]:
    """Send a separate copy of the input to each child layer, and join the
    outputs of the children into a tuple on the way out.

    Typically used to provide both modified data and the original input to a
    downstream layer.
    """

    layers = (layer1, layer2) + layers
    names = [layer.name for layer in layers]
    return Model(
        "tuple(" + ", ".join(names) + ")",
        tuplify_forward,
        init=init,
        layers=layers,
        dims={"nI": None},
    )


def tuplify_forward(model, X, is_train):
    Ys = []
    backprops = []
    for layer in model.layers:
        Y, backprop = layer(X, is_train)
        Ys.append(Y)
        backprops.append(backprop)

    def backprop_tuplify(dYs):
        dXs = [bp(dY) for bp, dY in zip(backprops, dYs)]
        dX = dXs[0]
        for dx in dXs[1:]:
            dX += dx
        return dX

    return tuple(Ys), backprop_tuplify


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is None and Y is None:
        for layer in model.layers:
            layer.initialize()
        if model.layers[0].has_dim("nI"):
            model.set_dim("nI", model.layers[0].get_dim("nI"))

    # Try to set nO on each layer, where available.
    # All layers have the same input, and the output should map directly from the
    # given Y, if provided.
    for ii, layer in enumerate(model.layers):
        if Y is not None and layer.has_dim("nO") is None:
            layer.initialize(X=X, Y=Y[ii])
        else:
            layer.initialize(X=X)

    if model.layers[0].has_dim("nI"):
        model.set_dim("nI", model.layers[0].get_dim("nI"))
    # this model can have an input dimension, but can't have an output dimension
