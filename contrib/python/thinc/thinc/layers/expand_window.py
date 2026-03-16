from typing import Callable, Tuple, TypeVar, Union, cast

from ..config import registry
from ..model import Model
from ..types import Floats2d, Ragged

InT = TypeVar("InT", Floats2d, Ragged)


@registry.layers("expand_window.v1")
def expand_window(window_size: int = 1) -> Model[InT, InT]:
    """For each vector in an input, construct an output vector that contains the
    input and a window of surrounding vectors. This is one step in a convolution.
    """
    return Model("expand_window", forward, attrs={"window_size": window_size})


def forward(model: Model[InT, InT], X: InT, is_train: bool) -> Tuple[InT, Callable]:
    if isinstance(X, Ragged):
        return _expand_window_ragged(model, X)
    else:
        return _expand_window_floats(model, X)


def _expand_window_floats(
    model: Model[InT, InT], X: Floats2d
) -> Tuple[Floats2d, Callable]:
    nW = model.attrs["window_size"]
    if len(X) > 0:
        Y = model.ops.seq2col(X, nW)
    else:
        assert len(X) == 0
        Y = model.ops.tile(X, (nW * 2) + 1)

    def backprop(dY: Floats2d) -> Floats2d:
        return model.ops.backprop_seq2col(dY, nW)

    return Y, backprop


def _expand_window_ragged(
    model: Model[InT, InT], Xr: Ragged
) -> Tuple[Ragged, Callable]:
    nW = model.attrs["window_size"]
    Y = Ragged(
        model.ops.seq2col(cast(Floats2d, Xr.data), nW, lengths=Xr.lengths), Xr.lengths
    )

    def backprop(dYr: Ragged) -> Ragged:
        return Ragged(
            model.ops.backprop_seq2col(
                cast(Floats2d, dYr.data), nW, lengths=Xr.lengths
            ),
            Xr.lengths,
        )

    return Y, backprop
