from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar, cast

from ..config import registry
from ..model import Model
from ..types import ArrayXd, ListXd

ItemT = TypeVar("ItemT")
InT = Sequence[Sequence[ItemT]]
OutT = ListXd
InnerInT = Sequence[ItemT]
InnerOutT = ArrayXd


@registry.layers("with_flatten.v1")
def with_flatten(layer: Model[InnerInT[ItemT], InnerOutT]) -> Model[InT[ItemT], OutT]:
    return Model(f"with_flatten({layer.name})", forward, layers=[layer], init=init)


def forward(
    model: Model[InT, OutT], Xnest: InT, is_train: bool
) -> Tuple[OutT, Callable]:
    layer: Model[InnerInT, InnerOutT] = model.layers[0]
    Xflat = _flatten(Xnest)
    Yflat, backprop_layer = layer(Xflat, is_train)
    # Get the split points. We want n-1 splits for n items.
    arr = layer.ops.asarray1i([len(x) for x in Xnest[:-1]])
    splits = arr.cumsum()
    Ynest = layer.ops.xp.split(Yflat, splits, axis=0)

    def backprop(dYnest: OutT) -> InT:
        dYflat = model.ops.flatten(dYnest)  # type: ignore[arg-type, var-annotated]
        # type ignore necessary for older versions of Mypy/Pydantic
        dXflat = backprop_layer(dYflat)
        dXnest = layer.ops.xp.split(dXflat, splits, axis=-1)
        return dXnest

    return Ynest, backprop


def _flatten(nested: InT) -> InnerInT:
    flat: List = []
    for item in nested:
        flat.extend(item)
    return cast(InT, flat)


def init(
    model: Model[InT, OutT], X: Optional[InT] = None, Y: Optional[OutT] = None
) -> None:
    model.layers[0].initialize(
        _flatten(X) if X is not None else None,
        model.layers[0].ops.xp.hstack(Y) if Y is not None else None,
    )
