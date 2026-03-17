from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from ..backends import NumpyOps
from ..config import registry
from ..model import Model
from ..types import Array2d, Ragged, XY_XY_OutT
from ..util import get_width
from .noop import noop

NUMPY_OPS = NumpyOps()


InT = TypeVar("InT", bound=Any)
OutT = TypeVar("OutT", bound=Union[Array2d, Sequence[Array2d], Ragged])


@registry.layers("concatenate.v1")
def concatenate(*layers: Model) -> Model[InT, XY_XY_OutT]:
    """Compose two or more models `f`, `g`, etc, such that their outputs are
    concatenated, i.e. `concatenate(f, g)(x)` computes `hstack(f(x), g(x))`.
    Also supports chaining more than 2 layers.
    """
    if not layers:
        return cast(Model[InT, XY_XY_OutT], noop())
    elif len(layers) == 1:
        return layers[0]
    elif layers[0]._func is forward:
        layers[0].layers.extend(layers[1:])
        return layers[0]

    # only add an nI dimension if each sub-layer has one
    dims: Dict[str, Optional[int]] = {"nO": None}
    if all(node.has_dim("nI") in [True, None] for node in layers):
        dims = {"nO": None, "nI": None}

    return Model(
        "|".join(layer.name for layer in layers),
        forward,
        init=init,
        dims=dims,
        layers=layers,
    )


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    Ys, callbacks = zip(*[layer(X, is_train=is_train) for layer in model.layers])
    if isinstance(Ys[0], list):
        data_l, backprop = _list_forward(model, X, Ys, callbacks, is_train)
        return cast(OutT, data_l), backprop
    elif isinstance(Ys[0], Ragged):
        data_r, backprop = _ragged_forward(model, X, Ys, callbacks, is_train)
        return cast(OutT, data_r), backprop
    else:
        data_a, backprop = _array_forward(model, X, Ys, callbacks, is_train)
        return cast(OutT, data_a), backprop


def _array_forward(
    model: Model[InT, OutT], X, Ys: List, callbacks, is_train: bool
) -> Tuple[Array2d, Callable]:
    widths = [Y.shape[1] for Y in Ys]
    output = model.ops.xp.hstack(Ys)

    def backprop(d_output: Array2d) -> InT:
        dY = model.ops.as_contig(d_output[:, : widths[0]])
        dX = callbacks[0](dY)
        start = widths[0]
        add_gradients = hasattr(dX, "__add__") or hasattr(dX, "__iadd__")
        add_gradients_data = hasattr(dX, "data") and (
            hasattr(dX.data, "__add__") or hasattr(dX.data, "__iadd__")
        )
        for bwd, width in zip(callbacks[1:], widths[1:]):
            dY = model.ops.as_contig(d_output[:, start : start + width])
            gradient = bwd(dY)
            if add_gradients:
                dX += gradient
            elif add_gradients_data:
                dX.data += gradient.data
            start += width
        return dX

    return output, backprop


def _ragged_forward(
    model: Model[InT, OutT], X, Ys: List, callbacks, is_train: bool
) -> Tuple[Ragged, Callable]:

    widths = [Y.dataXd.shape[1] for Y in Ys]
    output = Ragged(model.ops.xp.hstack([y.data for y in Ys]), Ys[0].lengths)

    def backprop(d_output: Ragged) -> InT:
        d_array = d_output.data
        dY = Ragged(model.ops.as_contig(d_array[:, : widths[0]]), d_output.lengths)
        dX = callbacks[0](dY)
        start = widths[0]
        for bwd, width in zip(callbacks[1:], widths[1:]):
            dY = Ragged(
                model.ops.as_contig(d_array[:, start : start + width]), d_output.lengths
            )
            dX += bwd(dY)
            start += width
        return dX

    return output, backprop


def _list_forward(
    model: Model[InT, OutT], X, Ys: List, callbacks, is_train: bool
) -> Tuple[Sequence[Array2d], Callable]:
    def backprop(d_output: Sequence[Array2d]) -> InT:
        d_out_array = model.ops.xp.concatenate(d_output, axis=0)
        dY = model.ops.as_contig(d_out_array[:, : widths[0]])
        # We want to generalize unflatten later.
        dY = model.ops.unflatten(dY, lengths)
        dX = callbacks[0](dY)
        start = widths[0]
        for bwd, width in zip(callbacks[1:], widths[1:]):
            dY = model.ops.as_contig(d_out_array[:, start : start + width])
            dY = model.ops.unflatten(dY, lengths)
            dX += bwd(dY)
            start += width
        return dX

    lengths = NUMPY_OPS.asarray1i([len(x) for x in X])
    Ys = [model.ops.xp.concatenate(Y, axis=0) for Y in Ys]
    widths = [Y.shape[1] for Y in Ys]
    out_array = model.ops.xp.hstack(Ys)
    return model.ops.unflatten(out_array, lengths), backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is not None:
        if model.has_dim("nI") is not False:
            model.set_dim("nI", get_width(X))
        for layer in model.layers:
            if layer.has_dim("nI") is not False:
                layer.set_dim("nI", get_width(X))
    for layer in model.layers:
        layer.initialize(X=X, Y=Y)
    if all([layer.has_dim("nO") for layer in model.layers]):
        model.set_dim("nO", sum(layer.get_dim("nO") for layer in model.layers))
