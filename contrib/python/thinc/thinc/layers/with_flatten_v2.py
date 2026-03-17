from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar, cast

from ..config import registry
from ..model import Model

InItemT = TypeVar("InItemT")
OutItemT = TypeVar("OutItemT")
ItemT = TypeVar("ItemT")

NestedT = List[List[ItemT]]
FlatT = List[ItemT]


@registry.layers("with_flatten.v2")
def with_flatten_v2(
    layer: Model[FlatT[InItemT], FlatT[OutItemT]]
) -> Model[NestedT[InItemT], NestedT[OutItemT]]:
    return Model(f"with_flatten({layer.name})", forward, layers=[layer], init=init)


def forward(
    model: Model[NestedT[InItemT], NestedT[OutItemT]],
    Xnest: NestedT[InItemT],
    is_train: bool,
) -> Tuple[NestedT[OutItemT], Callable]:
    layer: Model[FlatT[InItemT], FlatT[OutItemT]] = model.layers[0]
    Xflat, lens = _flatten(Xnest)
    Yflat, backprop_layer = layer(Xflat, is_train)
    Ynest = _unflatten(Yflat, lens)

    def backprop(dYnest: NestedT[InItemT]) -> NestedT[OutItemT]:
        dYflat, _ = _flatten(dYnest)  # type: ignore[arg-type, var-annotated]
        # type ignore necessary for older versions of Mypy/Pydantic
        dXflat = backprop_layer(dYflat)
        dXnest = _unflatten(dXflat, lens)
        return dXnest

    return Ynest, backprop


def _flatten(nested: NestedT[ItemT]) -> Tuple[FlatT[ItemT], List[int]]:
    flat: List = []
    lens: List[int] = []
    for item in nested:
        flat.extend(item)
        lens.append(len(item))
    return cast(FlatT[ItemT], flat), lens


def _unflatten(flat: FlatT[ItemT], lens: List[int]) -> NestedT[ItemT]:
    nested = []
    for l in lens:
        nested.append(flat[:l])
        flat = flat[l:]
    return nested


def init(
    model: Model[NestedT[InItemT], NestedT[OutItemT]],
    X: Optional[NestedT[InItemT]] = None,
    Y: Optional[NestedT[OutItemT]] = None,
) -> None:
    model.layers[0].initialize(
        _flatten(X)[0] if X is not None else None,
        model.layers[0].ops.xp.hstack(Y) if Y is not None else None,
    )
