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


@registry.layers("Relu.v1")
def Relu(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
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
        "relu",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
    )
    if normalize:
        model = chain(model, LayerNorm(nI=nO))
    if dropout is not None:
        model = chain(model, cast(Model[Floats2d, Floats2d], Dropout(dropout)))
    return model


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y = model.ops.affine(X, W, b)
    Y = model.ops.relu(Y)

    def backprop(dY: OutT) -> InT:
        dY = model.ops.backprop_relu(dY, Y)
        model.inc_grad("b", dY.sum(axis=0))
        model.inc_grad("W", model.ops.gemm(dY, X, trans1=True))
        return model.ops.gemm(dY, W)

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
