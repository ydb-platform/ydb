from typing import Callable, Optional, Tuple, TypeVar, Union, cast

from ..backends import NumpyOps
from ..config import registry
from ..model import Model
from ..types import Array3d, ArrayXd, ListXd, Padded, Ragged

NUMPY_OPS = NumpyOps()


ArrayTXd = TypeVar("ArrayTXd", bound=ArrayXd)
SeqT = TypeVar("SeqT", bound=Union[Padded, Ragged, ListXd, ArrayXd])


@registry.layers("with_array.v1")
def with_array(layer: Model[ArrayTXd, ArrayTXd], pad: int = 0) -> Model[SeqT, SeqT]:
    """Transform sequence data into a contiguous array on the way into and
    out of a model. Handles a variety of sequence types: lists, padded and ragged.
    If the input is an array, it is passed through unchanged.
    """
    model: Model[SeqT, SeqT] = Model(
        f"with_array({layer.name})",
        forward,
        init=init,
        layers=[layer],
        attrs={"pad": pad},
        dims={name: layer.maybe_get_dim(name) for name in layer.dim_names},
    )
    return model


def forward(
    model: Model[SeqT, SeqT], Xseq: SeqT, is_train: bool
) -> Tuple[SeqT, Callable]:
    if isinstance(Xseq, Ragged):
        return cast(Tuple[SeqT, Callable], _ragged_forward(model, Xseq, is_train))
    elif isinstance(Xseq, Padded):
        return cast(Tuple[SeqT, Callable], _padded_forward(model, Xseq, is_train))
    elif not isinstance(Xseq, (list, tuple)):
        return model.layers[0](Xseq, is_train)
    else:
        return cast(Tuple[SeqT, Callable], _list_forward(model, Xseq, is_train))


def init(
    model: Model[SeqT, SeqT], X: Optional[SeqT] = None, Y: Optional[SeqT] = None
) -> None:
    layer: Model[ArrayXd, ArrayXd] = model.layers[0]
    layer.initialize(
        X=_get_array(model, X) if X is not None else X,
        Y=_get_array(model, Y) if Y is not None else Y,
    )
    for dim_name in layer.dim_names:
        value = layer.maybe_get_dim(dim_name)
        if value is not None:
            model.set_dim(dim_name, value)


def _get_array(model, X: SeqT) -> ArrayXd:
    if isinstance(X, Ragged):
        return X.dataXd
    elif isinstance(X, Padded):
        return X.data
    elif not isinstance(X, (list, tuple)):
        return cast(ArrayXd, X)
    else:
        return model.ops.flatten(X)


def _list_forward(
    model: Model[SeqT, SeqT], Xs: ListXd, is_train: bool
) -> Tuple[ListXd, Callable]:
    layer: Model[ArrayXd, ArrayXd] = model.layers[0]
    pad = model.attrs["pad"]
    lengths = NUMPY_OPS.asarray1i([len(seq) for seq in Xs])
    Xf = layer.ops.flatten(Xs, pad=pad)
    Yf, get_dXf = layer(Xf, is_train)

    def backprop(dYs: ListXd) -> ListXd:
        dYf = layer.ops.flatten(dYs, pad=pad)
        dXf = get_dXf(dYf)
        return layer.ops.unflatten(dXf, lengths, pad=pad)

    return layer.ops.unflatten(Yf, lengths, pad=pad), backprop


def _ragged_forward(
    model: Model[SeqT, SeqT], Xr: Ragged, is_train: bool
) -> Tuple[Ragged, Callable]:
    layer: Model[ArrayXd, ArrayXd] = model.layers[0]
    Y, get_dX = layer(Xr.dataXd, is_train)

    def backprop(dYr: Ragged) -> Ragged:
        return Ragged(get_dX(dYr.dataXd), dYr.lengths)

    return Ragged(Y, Xr.lengths), backprop


def _padded_forward(
    model: Model[SeqT, SeqT], Xp: Padded, is_train: bool
) -> Tuple[Padded, Callable]:
    layer: Model[Array3d, Array3d] = model.layers[0]
    Y, get_dX = layer(Xp.data, is_train)

    def backprop(dYp: Padded) -> Padded:
        assert isinstance(dYp, Padded)
        dX = get_dX(dYp.data)
        return Padded(dX, dYp.size_at_t, dYp.lengths, dYp.indices)

    return Padded(Y, Xp.size_at_t, Xp.lengths, Xp.indices), backprop
