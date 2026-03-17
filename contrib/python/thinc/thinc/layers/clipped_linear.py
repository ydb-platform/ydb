from typing import Callable, Optional, Tuple, cast

from ..config import registry
from ..initializers import glorot_uniform_init, zero_init
from ..model import Model
from ..types import Floats1d, Floats2d
from ..util import get_width, partial
from .chain import chain
from .dropout import Dropout
from .layernorm import LayerNorm


@registry.layers("ClippedLinear.v1")
def ClippedLinear(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
    slope: float = 1.0,
    offset: float = 0.0,
    min_val: float = 0.0,
    max_val: float = 1.0,
) -> Model[Floats2d, Floats2d]:
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    model_attrs = {
        "slope": slope,
        "offset": offset,
        "min_val": min_val,
        "max_val": max_val,
    }
    model: Model[Floats2d, Floats2d] = Model(
        "clipped_linear",
        forward=forward,
        init=partial(init, init_W, init_b),
        dims={"nO": nO, "nI": nI},
        params={"W": None, "b": None},
        attrs=model_attrs,
    )
    if normalize:
        model = chain(model, LayerNorm(nI=nO))
    if dropout is not None:
        model = chain(model, cast(Model[Floats2d, Floats2d], Dropout(dropout)))
    return model


def forward(
    model: Model[Floats2d, Floats2d],
    X: Floats2d,
    is_train: bool,
) -> Tuple[Floats2d, Callable]:
    slope = model.attrs["slope"]
    offset = model.attrs["offset"]
    min_val = model.attrs["min_val"]
    max_val = model.attrs["max_val"]
    W = cast(Floats2d, model.get_param("W"))
    b = cast(Floats1d, model.get_param("b"))
    Y_preact = model.ops.affine(X, W, b)
    Y = model.ops.clipped_linear(Y_preact, slope, offset, min_val, max_val)

    def backprop(dY: Floats2d) -> Floats2d:
        dY = model.ops.backprop_clipped_linear(
            dY, Y_preact, slope, offset, min_val, max_val, inplace=False
        )
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


@registry.layers("HardSigmoid.v1")
def HardSigmoid(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
) -> Model[Floats2d, Floats2d]:
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    return ClippedLinear(
        nO=nO,
        nI=nI,
        init_W=init_W,
        dropout=dropout,
        normalize=normalize,
        slope=0.2,
        offset=0.5,
    )


@registry.layers("HardTanh.v1")
def HardTanh(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
) -> Model[Floats2d, Floats2d]:
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    return ClippedLinear(
        nO=nO,
        nI=nI,
        init_W=init_W,
        dropout=dropout,
        normalize=normalize,
        min_val=-1.0,
        max_val=1.0,
    )


@registry.layers("ReluK.v1")
def ReluK(
    nO: Optional[int] = None,
    nI: Optional[int] = None,
    *,
    init_W: Optional[Callable] = None,
    init_b: Optional[Callable] = None,
    dropout: Optional[float] = None,
    normalize: bool = False,
    k: float = 6.0,
) -> Model[Floats2d, Floats2d]:
    if init_W is None:
        init_W = glorot_uniform_init
    if init_b is None:
        init_b = zero_init
    return ClippedLinear(
        nO=nO,
        nI=nI,
        init_W=init_W,
        dropout=dropout,
        normalize=normalize,
        min_val=0.0,
        max_val=k,
    )
