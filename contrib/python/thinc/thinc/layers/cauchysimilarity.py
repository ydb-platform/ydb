from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width

InT = Tuple[Floats2d, Floats2d]
OutT = Floats1d


@registry.layers("CauchySimilarity.v1")
def CauchySimilarity(nI: Optional[int] = None) -> Model[InT, OutT]:
    """Compare input vectors according to the Cauchy similarity function proposed by
    Chen (2013). Primarily used within Siamese neural networks.
    """
    return Model(
        "cauchy_similarity",
        forward,
        init=init,
        dims={"nI": nI, "nO": 1},
        params={"W": None},
    )


def forward(
    model: Model[InT, OutT], X1_X2: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    X1, X2 = X1_X2
    W = cast(Floats2d, model.get_param("W"))
    diff = X1 - X2
    square_diff = diff**2
    total = (W * square_diff).sum(axis=1)
    sim, bp_sim = inverse(total)

    def backprop(d_sim: OutT) -> InT:
        d_total = bp_sim(d_sim)
        d_total = model.ops.reshape2f(d_total, -1, 1)
        model.inc_grad("W", (d_total * square_diff).sum(axis=0))
        d_square_diff = W * d_total
        d_diff = 2 * d_square_diff * diff
        return (d_diff, -d_diff)

    return sim, backprop


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is not None:
        model.set_dim("nI", get_width(X[0]))
    # Initialize weights to 1
    W = model.ops.alloc1f(model.get_dim("nI"))
    W += 1
    model.set_param("W", W)


def inverse(total: OutT) -> Tuple[OutT, Callable]:
    inv = 1.0 / (1 + total)

    def backward(d_inverse: OutT) -> OutT:
        return d_inverse * (-1 / (total + 1) ** 2)

    return inv, backward
