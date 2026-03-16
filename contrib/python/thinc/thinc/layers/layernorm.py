from typing import Callable, Optional, Tuple, cast

from ..backends import Ops
from ..config import registry
from ..model import Model
from ..types import Floats2d
from ..util import get_width

InT = Floats2d


@registry.layers("LayerNorm.v1")
def LayerNorm(nI: Optional[int] = None) -> Model[InT, InT]:
    return Model(
        "layernorm",
        forward,
        init=init,
        dims={"nI": nI, "nO": nI},
        params={"G": None, "b": None},
    )


def forward(model: Model[InT, InT], X: InT, is_train: bool) -> Tuple[InT, Callable]:
    N, mu, var = _get_moments(model.ops, X)
    Xhat = (X - mu) * var ** (-1.0 / 2.0)
    Y, backprop_rescale = _begin_update_scale_shift(model, Xhat)

    def backprop(dY: InT) -> InT:
        dY = backprop_rescale(dY)
        dist, sum_dy, sum_dy_dist = _get_d_moments(model.ops, dY, X, mu)
        d_xhat = N * dY - sum_dy - dist * var ** (-1.0) * sum_dy_dist
        d_xhat *= var ** (-1.0 / 2)
        d_xhat /= N
        return d_xhat

    return Y, backprop


def init(
    model: Model[InT, InT], X: Optional[InT] = None, Y: Optional[InT] = None
) -> None:
    if X is not None:
        X_width = get_width(X)
        model.set_dim("nI", X_width)
        model.set_dim("nO", X_width)
    elif Y is not None:
        Y_width = get_width(Y)
        model.set_dim("nI", Y_width)
        model.set_dim("nO", Y_width)
    nI = model.get_dim("nI")
    if not model.has_dim("nO"):
        model.set_dim("nO", nI)
    model.set_param("G", model.ops.alloc1f(nI) + 1)
    model.set_param("b", model.ops.alloc1f(nI))
    assert model.get_dim("nO") is not None


def _begin_update_scale_shift(model: Model[InT, InT], X: InT) -> Tuple[InT, Callable]:
    G = model.get_param("G")
    b = model.get_param("b")
    Y = X * G
    Y += b

    def finish_update_scale_shift(dY: InT) -> InT:
        model.inc_grad("b", dY.sum(axis=0))
        model.inc_grad("G", (dY * X).sum(axis=0))
        return dY * G

    return Y, finish_update_scale_shift


def _get_moments(ops: Ops, X: Floats2d) -> Tuple[Floats2d, Floats2d, Floats2d]:
    # TODO: Do mean methods
    mu: Floats2d = X.mean(axis=1, keepdims=True)
    var: Floats2d = X.var(axis=1, keepdims=True) + 1e-08
    return cast(Floats2d, ops.asarray_f([X.shape[1]])), mu, var


def _get_d_moments(
    ops: Ops, dy: Floats2d, X: Floats2d, mu: Floats2d
) -> Tuple[Floats2d, Floats2d, Floats2d]:
    dist = X - mu
    return (
        dist,
        ops.xp.sum(dy, axis=1, keepdims=True),
        ops.xp.sum(dy * dist, axis=1, keepdims=True),
    )
