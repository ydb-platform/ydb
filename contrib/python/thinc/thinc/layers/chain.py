from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, cast

from ..config import registry
from ..model import Model
from ..types import XY_YZ_OutT
from ..util import get_width

InT = TypeVar("InT")
MidT = TypeVar("MidT")
OutT = TypeVar("OutT")

# Keep this function so we can provide variable arguments via the config
@registry.layers("chain.v1")
def chain_no_types(*layer: Model) -> Model:
    return chain(*layer)


def chain(
    layer1: Model[InT, MidT], layer2: Model[MidT, Any], *layers: Model[Any, Any]
) -> Model[InT, XY_YZ_OutT]:
    """Compose two models `f` and `g` such that they become layers of a single
    feed-forward model that computes `g(f(x))`.
    Also supports chaining more than 2 layers.
    Note that the type checking for additional layers is carried out by the Thinc Mypy plugin.
    """
    all_layers: List[Model[Any, Any]] = [layer1, layer2]
    all_layers.extend(layers)
    dims: Dict[str, Optional[int]] = {"nO": None}
    # set input dimension only if first layer has one - should be "False" otherwise
    if all_layers[0].has_dim("nI") is True:
        dims["nI"] = all_layers[0].get_dim("nI")
    if all_layers[0].has_dim("nI") is None:
        dims["nI"] = None
    # set output dimension according to last layer
    if all_layers[-1].has_dim("nO") is True:
        dims["nO"] = all_layers[-1].get_dim("nO")

    model: Model[InT, XY_YZ_OutT] = Model(
        ">>".join(layer.name for layer in all_layers),
        forward,
        init=init,
        dims=dims,
        layers=all_layers,
    )
    return model


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    """Apply the layers of `model` in sequence, feeding the output from one
    layer into the next.
    """
    callbacks = []
    for layer in model.layers:
        Y, inc_layer_grad = layer(X, is_train=is_train)
        callbacks.append(inc_layer_grad)
        X = Y

    def backprop(dY: OutT) -> InT:
        for callback in reversed(callbacks):
            dX = callback(dY)
            dY = dX
        return dX

    return Y, backprop


def init(
    model: Model[InT, OutT],
    X: Optional[InT] = None,
    Y: Optional[OutT] = None,
) -> None:
    if X is None and Y is None:
        for layer in model.layers:
            layer.initialize()
        if model.layers[0].has_dim("nI"):
            model.set_dim("nI", model.layers[0].get_dim("nI"))
        if model.layers[-1].has_dim("nO"):
            model.set_dim("nO", model.layers[-1].get_dim("nO"))

    # Try to set nO on each layer, where available.
    # Shape inference is tricky, especially for the output. The policy is:
    # if a layer has an unset nO, we use the final Y (if provided). For other
    # layers, Y=None.
    curr_input = X
    for layer in model.layers:
        if layer.has_dim("nO") is None:
            layer.initialize(X=curr_input, Y=Y)
        else:
            layer.initialize(X=curr_input)
        if curr_input is not None:
            curr_input = layer.predict(curr_input)

    if model.layers[0].has_dim("nI"):
        model.set_dim("nI", model.layers[0].get_dim("nI"))
    if model.has_dim("nO") is None:
        try:
            nO = get_width(curr_input)  # type: ignore[arg-type]
            model.set_dim("nO", nO)
        except ValueError:
            if model.layers[-1].has_dim("nO"):
                nO = model.layers[-1].get_dim("nO")
                model.set_dim("nO", nO)
