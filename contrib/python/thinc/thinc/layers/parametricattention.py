from typing import Callable, Optional, Tuple

from ..config import registry
from ..model import Model
from ..types import Ragged
from ..util import get_width

InT = Ragged
OutT = Ragged


@registry.layers("ParametricAttention.v1")
def ParametricAttention(nO: Optional[int] = None) -> Model[InT, OutT]:
    """Weight inputs by similarity to a learned vector"""
    return Model("para-attn", forward, init=init, params={"Q": None}, dims={"nO": nO})


def forward(model: Model[InT, OutT], Xr: InT, is_train: bool) -> Tuple[OutT, Callable]:
    Q = model.get_param("Q")
    attention, bp_attention = _get_attention(model.ops, Q, Xr.dataXd, Xr.lengths)
    output, bp_output = _apply_attention(model.ops, attention, Xr.dataXd, Xr.lengths)

    def backprop(dYr: OutT) -> InT:
        dX, d_attention = bp_output(dYr.dataXd)
        dQ, dX2 = bp_attention(d_attention)
        model.inc_grad("Q", dQ.ravel())
        dX += dX2
        return Ragged(dX, dYr.lengths)

    return Ragged(output, Xr.lengths), backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is not None:
        model.set_dim("nO", get_width(X))
    # Randomly initialize the parameter, as though it were an embedding.
    Q = model.ops.alloc1f(model.get_dim("nO"))
    Q += model.ops.xp.random.uniform(-0.1, 0.1, Q.shape)
    model.set_param("Q", Q)


def _get_attention(ops, Q, X, lengths):
    attention = ops.gemm(X, ops.reshape2f(Q, -1, 1))
    attention = ops.softmax_sequences(attention, lengths)

    def get_attention_bwd(d_attention):
        d_attention = ops.backprop_softmax_sequences(d_attention, attention, lengths)
        dQ = ops.gemm(X, d_attention, trans1=True)
        dX = ops.xp.outer(d_attention, Q)
        return dQ, dX

    return attention, get_attention_bwd


def _apply_attention(ops, attention, X, lengths):
    output = X * attention

    def apply_attention_bwd(d_output):
        d_attention = (X * d_output).sum(axis=1, keepdims=True)
        dX = d_output * attention
        return dX, d_attention

    return output, apply_attention_bwd
