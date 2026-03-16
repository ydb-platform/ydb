from typing import Callable, Optional, TypeVar

from ..config import registry
from ..model import Model
from ..types import Floats2d

InT = TypeVar("InT")
OutT = TypeVar("OutT")


@registry.layers("resizable.v1")
def resizable(layer, resize_layer: Callable) -> Model[InT, OutT]:
    """Container that holds one layer that can change dimensions."""
    return Model(
        f"resizable({layer.name})",
        forward,
        init=init,
        layers=[layer],
        attrs={"resize_layer": resize_layer},
        dims={name: layer.maybe_get_dim(name) for name in layer.dim_names},
    )


def forward(model: Model[InT, OutT], X: InT, is_train: bool):
    layer = model.layers[0]
    Y, callback = layer(X, is_train=is_train)

    def backprop(dY: OutT) -> InT:
        return callback(dY)

    return Y, backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    layer = model.layers[0]
    layer.initialize(X, Y)


def resize_model(model: Model[InT, OutT], new_nO):
    old_layer = model.layers[0]
    new_layer = model.attrs["resize_layer"](old_layer, new_nO)
    model.layers[0] = new_layer
    return model


def resize_linear_weighted(
    layer: Model[Floats2d, Floats2d], new_nO, *, fill_defaults=None
) -> Model[Floats2d, Floats2d]:
    """Create a resized copy of a layer that has parameters W and b and dimensions nO and nI."""
    assert not layer.layers
    assert not layer.ref_names
    assert not layer.shims

    # return the original layer if it wasn't initialized or if nO didn't change
    if layer.has_dim("nO") is None:
        layer.set_dim("nO", new_nO)
        return layer
    elif new_nO == layer.get_dim("nO"):
        return layer
    elif layer.has_dim("nI") is None:
        layer.set_dim("nO", new_nO, force=True)
        return layer

    dims = {name: layer.maybe_get_dim(name) for name in layer.dim_names}
    dims["nO"] = new_nO
    new_layer: Model[Floats2d, Floats2d] = Model(
        layer.name,
        layer._func,
        dims=dims,
        params={name: None for name in layer.param_names},
        init=layer.init,
        attrs=layer.attrs,
        refs={},
        ops=layer.ops,
    )
    new_layer.initialize()
    for name in layer.param_names:
        if layer.has_param(name):
            filler = 0 if not fill_defaults else fill_defaults.get(name, 0)
            _resize_parameter(name, layer, new_layer, filler=filler)
    return new_layer


def _resize_parameter(name, layer, new_layer, filler=0):
    larger = new_layer.get_param(name)
    smaller = layer.get_param(name)
    # copy the original weights
    larger[: len(smaller)] = smaller
    # set the new weights
    larger[len(smaller) :] = filler
    new_layer.set_param(name, larger)
