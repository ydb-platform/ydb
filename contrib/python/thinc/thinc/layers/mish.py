from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import glorot_uniform_init, zero_init
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width, partial
from .chain import chain
from .dropout import Dropout
from .layernorm import LayerNorm

InT = Floats2d
OutT = Floats2d


@registry.layers("Mish.v1")
def Mish(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
) -> Model[InT, OutT]:
    """Dense layer with mish activation.
    https://arxiv.org/pdf/1908.08681.pdf
    """
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    model: Model[InT, OutT] = Model(
        "mish",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
    )
    if normalize:
        model = chain(model, cast(Model[InT, OutT], LayerNorm(nI=nO)))
    if dropout is not None:
        model = chain(model, cast(Model[InT, OutT], Dropout(dropout)))
    return model


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y_pre_mish = model.ops.gemm(X, W, trans2=True)
    Y_pre_mish += b
    Y = model.ops.mish(Y_pre_mish)

    def backprop(dY: OutT) -> InT:
        dY_pre_mish = model.ops.backprop_mish(dY, Y_pre_mish)
        model.inc_grad("W", model.ops.gemm(dY_pre_mish, X, trans1=True))
        model.inc_grad("b", dY_pre_mish.sum(axis=0))
        dX = model.ops.gemm(dY_pre_mish, W)
        return dX

    return Y, backprop


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
    model.set_param("W", init_W(model.ops, (model.get_dim("nO"), model.get_dim("nI"))))
    model.set_param("b", init_b(model.ops, (model.get_dim("nO"),)))
