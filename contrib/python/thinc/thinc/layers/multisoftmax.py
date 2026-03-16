from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width

InT = Floats2d
OutT = Floats2d


@registry.layers("MultiSoftmax.v1")
def MultiSoftmax(nOs: Tuple[int, ...], nI: Optional[int] = None) -> Model[InT, OutT]:
    """Neural network layer that predicts several multi-class attributes at once.
    For instance, we might predict one class with 6 variables, and another with 5.
    We predict the 11 neurons required for this, and then softmax them such
    that columns 0-6 make a probability distribution and columns 6-11 make another.
    """
    return Model(
        "multisoftmax",
        forward,
        init=init,
        dims={"nO": sum(nOs), "nI": nI},
        attrs={"nOs": nOs},
        params={"W": None, "b": None},
    )


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    nOs = model.attrs["nOs"]
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))

    def backprop(dY: OutT) -> InT:
        model.inc_grad("W", model.ops.gemm(dY, X, trans1=True))
        model.inc_grad("b", dY.sum(axis=0))
        return model.ops.gemm(dY, W)

    Y = model.ops.gemm(X, W, trans2=True)
    Y += b
    i = 0
    for out_size in nOs:
        model.ops.softmax(Y[:, i : i + out_size], inplace=True)
        i += out_size
    return Y, backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is not None:
        model.set_dim("nI", get_width(X))
    nO = model.get_dim("nO")
    nI = model.get_dim("nI")
    model.set_param("W", model.ops.alloc2f(nO, nI))
    model.set_param("b", model.ops.alloc1f(nO))
