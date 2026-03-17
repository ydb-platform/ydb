from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import glorot_uniform_init, zero_init
from ..model import Model
from ..types import Floats2d
from ..util import get_width, partial
from .chain import chain
from .dropout import Dropout
from .layernorm import LayerNorm

InT = Floats2d
OutT = Floats2d


@registry.layers("Maxout.v1")
def Maxout(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    nP: Optional[int] = 3,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
) -> Model[InT, OutT]:
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    model: Model[InT, OutT] = Model(
        "maxout",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI, "nP": nP},
        params={"W": None, "b": None},
    )
    if normalize:
        model = chain(model, LayerNorm(nI=nO))
    if dropout is not None:
        model = chain(model, cast(Model[InT, OutT], Dropout(dropout)))
    return model


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    nO = model.get_dim("nO")
    nP = model.get_dim("nP")
    nI = model.get_dim("nI")
    b = model.get_param("b")
    W = model.get_param("W")
    W = model.ops.reshape2f(W, nO * nP, nI)
    Y = model.ops.gemm(X, W, trans2=True)
    Y += model.ops.reshape1f(b, nO * nP)
    Z = model.ops.reshape3f(Y, Y.shape[0], nO, nP)
    best, which = model.ops.maxout(Z)

    def backprop(d_best: OutT) -> InT:
        dZ = model.ops.backprop_maxout(d_best, which, nP)
        # TODO: Add sum methods for Floats3d
        model.inc_grad("b", dZ.sum(axis=0))  # type: ignore[call-overload]
        dY = model.ops.reshape2f(dZ, dZ.shape[0], nO * nP)
        dW = model.ops.reshape3f(model.ops.gemm(dY, X, trans1=True), nO, nP, nI)
        model.inc_grad("W", dW)
        return model.ops.gemm(dY, model.ops.reshape2f(W, nO * nP, nI))

    return best, backprop


def init(
    init_W: Callable,
    init_b: Callable,
    model: Model[InT, OutT],
    X: Optional[InT] = None,
    Y: Optional[OutT] = None,
) -> None:
    if X is not None:
        model.set_dim("nI", get_width(X))
    if Y is not None:
        model.set_dim("nO", get_width(Y))
    W_shape = (model.get_dim("nO"), model.get_dim("nP"), model.get_dim("nI"))
    model.set_param("W", init_W(model.ops, W_shape))
    model.set_param("b", init_b(model.ops, (model.get_dim("nO"), model.get_dim("nP"))))
