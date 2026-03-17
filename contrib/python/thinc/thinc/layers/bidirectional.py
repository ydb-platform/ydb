from typing import Callable, Optional, Tuple, cast

from ..backends import Ops
from ..config import registry
from ..model import Model
from ..types import Padded

InT = Padded
OutT = Padded


@registry.layers("bidirectional.v1")
def bidirectional(
    l2r: Model[InT, OutT], r2l: Optional[Model[InT, OutT]] = None
) -> Model[InT, OutT]:
    """Stitch two RNN models into a bidirectional layer. Expects squared sequences."""
    if r2l is None:
        r2l = l2r.copy()
    return Model(f"bi{l2r.name}", forward, layers=[l2r, r2l], init=init)


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    l2r, r2l = model.layers
    X_rev = _reverse(model.ops, X)
    l2r_Z, bp_l2r_Z = l2r(X, is_train)
    r2l_Z, bp_r2l_Z = r2l(X_rev, is_train)
    Z = _concatenate(model.ops, l2r_Z, r2l_Z)

    def backprop(dZ: OutT) -> InT:
        d_l2r_Z, d_r2l_Z = _split(model.ops, dZ)
        dX_l2r = bp_l2r_Z(d_l2r_Z)
        dX_r2l = bp_r2l_Z(d_r2l_Z)
        return _sum(dX_l2r, dX_r2l)

    return Z, backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    (Y1, Y2) = _split(model.ops, Y) if Y is not None else (None, None)
    model.layers[0].initialize(X=X, Y=Y1)
    model.layers[1].initialize(X=X, Y=Y2)


def _reverse(ops: Ops, Xp: Padded) -> Padded:
    return Padded(Xp.data[::1], Xp.size_at_t, Xp.lengths, Xp.indices)


def _concatenate(ops: Ops, l2r: Padded, r2l: Padded) -> Padded:
    return Padded(
        ops.xp.concatenate((l2r.data, r2l.data), axis=-1),
        l2r.size_at_t,
        l2r.lengths,
        l2r.indices,
    )


def _split(ops: Ops, Xp: Padded) -> Tuple[Padded, Padded]:
    half = Xp.data.shape[-1] // 2
    # I don't know how to write these ellipsis in the overloads :(
    X_l2r = Xp.data[cast(Tuple[slice, slice], (..., slice(None, half)))]
    X_r2l = Xp.data[cast(Tuple[slice, slice], (..., slice(half)))]
    return (
        Padded(X_l2r, Xp.size_at_t, Xp.lengths, Xp.indices),
        Padded(X_r2l, Xp.size_at_t, Xp.lengths, Xp.indices),
    )


def _sum(Xp: Padded, Yp: Padded) -> Padded:
    return Padded(Xp.data + Yp.data, Xp.size_at_t, Xp.lengths, Xp.indices)
