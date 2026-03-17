from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats2d, Ragged
from ..util import get_width
from .noop import noop

InT = Ragged
OutT = Ragged

KEY_TRANSFORM_REF: str = "key_transform"


@registry.layers("ParametricAttention.v2")
def ParametricAttention_v2(
    *,
    key_transform: Optional[Model[Floats2d, Floats2d]] = None,
    nO: Optional[int] = None
) -> Model[InT, OutT]:
    if key_transform is None:
        key_transform = noop()

    """Weight inputs by similarity to a learned vector"""
    return Model(
        "para-attn",
        forward,
        init=init,
        params={"Q": None},
        dims={"nO": nO},
        refs={KEY_TRANSFORM_REF: key_transform},
        layers=[key_transform],
    )


def forward(model: Model[InT, OutT], Xr: InT, is_train: bool) -> Tuple[OutT, Callable]:
    Q = model.get_param("Q")
    key_transform = model.get_ref(KEY_TRANSFORM_REF)

    attention, bp_attention = _get_attention(
        model.ops, Q, key_transform, Xr.dataXd, Xr.lengths, is_train
    )
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
    key_transform = model.get_ref(KEY_TRANSFORM_REF)
    width = get_width(X) if X is not None else None
    if width:
        model.set_dim("nO", width)
        if key_transform.has_dim("nO"):
            key_transform.set_dim("nO", width)

    # Randomly initialize the parameter, as though it were an embedding.
    Q = model.ops.alloc1f(model.get_dim("nO"))
    Q += model.ops.xp.random.uniform(-0.1, 0.1, Q.shape)
    model.set_param("Q", Q)

    X_array = X.dataXd if X is not None else None
    Y_array = Y.dataXd if Y is not None else None

    key_transform.initialize(X_array, Y_array)


def _get_attention(ops, Q, key_transform, X, lengths, is_train):
    K, K_bp = key_transform(X, is_train=is_train)

    attention = ops.gemm(K, ops.reshape2f(Q, -1, 1))
    attention = ops.softmax_sequences(attention, lengths)

    def get_attention_bwd(d_attention):
        d_attention = ops.backprop_softmax_sequences(d_attention, attention, lengths)
        dQ = ops.gemm(K, d_attention, trans1=True)
        dY = ops.xp.outer(d_attention, Q)
        dX = K_bp(dY)
        return dQ, dX

    return attention, get_attention_bwd


def _apply_attention(ops, attention, X, lengths):
    output = X * attention

    def apply_attention_bwd(d_output):
        d_attention = (X * d_output).sum(axis=1, keepdims=True)
        dX = d_output * attention
        return dX, d_attention

    return output, apply_attention_bwd
