from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import zero_init
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import ArrayInfo, get_width, partial

InT = Floats2d
OutT = Floats2d


@registry.layers("Softmax.v1")
def Softmax(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
) -> Model[InT, OutT]:
    if init_W is None:
        init_W = zero_init
    if init_b is None:
        init_b = zero_init
    return Model(
        "softmax",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
        attrs={"softmax_normalize": True, "softmax_temperature": 1.0},
    )


@registry.layers("Softmax.v2")
def Softmax_v2(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    normalize_outputs: bool = True,
    temperature: float = 1.0,
) -> Model[InT, OutT]:
    if init_W is None:
        init_W = zero_init
    if init_b is None:
        init_b = zero_init
    validate_temperature(temperature)
    return Model(
        "softmax",
        forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
        attrs={
            "softmax_normalize": normalize_outputs,
            "softmax_temperature": temperature,
        },
    )


def forward(model: Model[InT, OutT], X: InT, is_train: bool) -> Tuple[OutT, Callable]:
    normalize = model.attrs["softmax_normalize"] or is_train

    temperature = model.attrs["softmax_temperature"]
    validate_temperature(temperature)

    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y = model.ops.affine(X, W, b)

    if normalize:
        Y = model.ops.softmax(Y, temperature=temperature)

    array_info = ArrayInfo.from_array(Y)

    def backprop(dY: InT) -> OutT:
        array_info.check_consistency(dY)
        if temperature != 1.0:
            dY = dY / temperature

        model.inc_grad("b", dY.sum(axis=0))
        model.inc_grad("W", model.ops.gemm(dY, X, trans1=True))
        return model.ops.gemm(dY, W)

    def backprop_unnormalized(dY: InT):
        msg = "backprop is not supported for an unnormalized Softmax layer"
        raise ValueError(msg)

    if normalize:
        return Y, backprop
    else:
        return Y, backprop_unnormalized


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


def validate_temperature(temperature):
    if temperature <= 0.0:
        msg = "softmax temperature must not be zero or negative"
        raise ValueError(msg)
