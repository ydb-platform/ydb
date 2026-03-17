from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import zero_init
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width, partial

InT = Floats2d
OutT = Floats2d


@registry.layers("Sigmoid.v1")
def Sigmoid(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
) -> Model[InT, OutT]:
    """A dense layer, followed by a sigmoid (logistic) activation function. This
    is usually used instead of the Softmax layer as an output for multi-label
    classification.
    """
    if init_W is None:
        init_W = zero_init
    if init_b is None:
        init_b = zero_init
    return Model(
        "sigmoid",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
    )


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y = model.ops.affine(X, W, b)
    Y = model.ops.sigmoid(Y)

    def backprop(dY: InT) -> OutT:
        dY = model.ops.backprop_sigmoid(dY, Y, inplace=False)
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
    if X is not None and model.has_dim("nI") is None:
        model.set_dim("nI", get_width(X))
    if Y is not None and model.has_dim("nO") is None:
        model.set_dim("nO", get_width(Y))
    model.set_param("W", init_W(model.ops, (model.get_dim("nO"), model.get_dim("nI"))))
    model.set_param("b", init_b(model.ops, (model.get_dim("nO"),)))
