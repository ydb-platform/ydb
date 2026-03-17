from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import he_normal_init, zero_init
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width, partial
from .chain import chain
from .dropout import Dropout
from .layernorm import LayerNorm


@registry.layers("Swish.v1")
def Swish(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
) -> Model[Floats2d, Floats2d]:
    if init_W is None:
        init_W = he_normal_init
    if init_b is None:
        init_b = zero_init
    model: Model[Floats2d, Floats2d] = Model(
        "swish",
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


def forward(
    model: Model[Floats2d, Floats2d], X: Floats2d, is_train: bool
) -> Tuple[Floats2d, Callable]:
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y_preact = model.ops.affine(X, W, b)
    Y = model.ops.swish(Y_preact)

    def backprop(dY: Floats2d) -> Floats2d:
        dY = model.ops.backprop_swish(dY, Y_preact, Y, inplace=False)
        model.inc_grad("b", dY.sum(axis=0))
        model.inc_grad("W", model.ops.gemm(dY, X, trans1=True))
        return model.ops.gemm(dY, W)

    return Y, backprop


def init(
    init_W: Callable,
    init_b: Callable,
    model: Model[Floats2d, Floats2d],
    X: Optional[Floats2d] = None,
    Y: Optional[Floats2d] = None,
) -> None:
    if X is not None:
        model.set_dim("nI", get_width(X))
    if Y is not None:
        model.set_dim("nO", get_width(Y))
    model.set_param("W", init_W(model.ops, (model.get_dim("nO"), model.get_dim("nI"))))
    model.set_param("b", init_b(model.ops, (model.get_dim("nO"),)))
