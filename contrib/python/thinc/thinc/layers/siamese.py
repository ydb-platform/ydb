from typing import Callable, Optional, Tuple, TypeVar

from ..config import registry
from ..model import Model
from ..types import ArrayXd
from ..util import get_width

LayerT = TypeVar("LayerT")
SimT = TypeVar("SimT")
InT = Tuple[LayerT, LayerT]
OutT = TypeVar("OutT", bound=ArrayXd)


@registry.layers("siamese.v1")
def siamese(
    layer: Model[LayerT, SimT], similarity: Model[Tuple[SimT, SimT], OutT]
) -> Model[InT, OutT]:
    return Model(
        f"siamese({layer.name}, {similarity.name})",
        forward,
        init=init,
        layers=[layer, similarity],
        dims={"nI": layer.get_dim("nI"), "nO": similarity.get_dim("nO")},
    )


def forward(
    model: Model[InT, OutT], X1_X2: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    X1, X2 = X1_X2
    vec1, bp_vec1 = model.layers[0](X1, is_train)
    vec2, bp_vec2 = model.layers[0](X2, is_train)
    output, bp_output = model.layers[1]((vec1, vec2), is_train)

    def finish_update(d_output: OutT) -> InT:
        d_vec1, d_vec2 = bp_output(d_output)
        d_input1 = bp_vec1(d_vec1)
        d_input2 = bp_vec2(d_vec2)
        return (d_input1, d_input2)

    return output, finish_update


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    if X is not None:
        model.layers[0].set_dim("nI", get_width(X[1]))
        model.layers[0].initialize(X=X[0])
        X = (model.layers[0].predict(X[0]), model.layers[0].predict(X[1]))
    model.layers[1].initialize(X=X, Y=Y)
    model.set_dim("nI", model.layers[0].get_dim("nI"))
    model.set_dim("nO", model.layers[1].get_dim("nO"))
