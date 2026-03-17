from typing import Callable, List, Optional, Tuple, TypeVar, Union, cast

from ..config import registry
from ..model import Model
from ..types import Array2d, Floats2d, Ints2d, List2d, Padded, Ragged

SeqT = TypeVar("SeqT", Padded, Ragged, List2d, List[Floats2d], List[Ints2d])


@registry.layers("with_list.v1")
def with_list(layer: Model[List2d, List2d]) -> Model[SeqT, SeqT]:
    return Model(
        f"with_list({layer.name})",
        forward,
        init=init,
        layers=[layer],
        dims={name: layer.maybe_get_dim(name) for name in layer.dim_names},
    )


def forward(
    model: Model[SeqT, SeqT], Xseq: SeqT, is_train: bool
) -> Tuple[SeqT, Callable]:
    layer: Model[List2d, List2d] = model.layers[0]
    if isinstance(Xseq, Padded):
        return _padded_forward(layer, Xseq, is_train)
    elif isinstance(Xseq, Ragged):
        return _ragged_forward(layer, Xseq, is_train)
    else:
        return cast(Tuple[SeqT, Callable], layer(cast(List2d, Xseq), is_train))


def init(
    model: Model[SeqT, SeqT], X: Optional[SeqT] = None, Y: Optional[SeqT] = None
) -> None:
    model.layers[0].initialize(
        X=_get_list(model, X) if X is not None else None,
        Y=_get_list(model, Y) if Y is not None else None,
    )


def _get_list(model, seq):
    if isinstance(seq, Padded):
        return model.ops.padded2list(seq)
    elif isinstance(seq, Ragged):
        return model.ops.unflatten(seq.data, seq.lengths)
    else:
        return seq


def _ragged_forward(
    layer: Model[List2d, List2d], Xr: Ragged, is_train: bool
) -> Tuple[Ragged, Callable]:
    # Assign these to locals, to keep code a bit shorter.
    unflatten = layer.ops.unflatten
    flatten = layer.ops.flatten
    # It's worth being a bit careful about memory here, as the activations
    # are potentially large on GPU. So we make nested function calls instead
    # of assigning to temporaries where possible, so memory can be reclaimed
    # sooner.
    Ys, get_dXs = layer(unflatten(Xr.data, Xr.lengths), is_train)

    def backprop(dYr: Ragged):
        return Ragged(
            flatten(get_dXs(unflatten(dYr.data, dYr.lengths))),
            dYr.lengths,
        )

    return Ragged(flatten(Ys), Xr.lengths), backprop


def _padded_forward(
    layer: Model[List2d, List2d], Xp: Padded, is_train: bool
) -> Tuple[Padded, Callable]:
    # Assign these to locals, to keep code a bit shorter.
    padded2list = layer.ops.padded2list
    list2padded = layer.ops.list2padded
    # It's worth being a bit careful about memory here, as the activations
    # are potentially large on GPU. So we make nested function calls instead
    # of assigning to temporaries where possible, so memory can be reclaimed
    # sooner.
    Ys, get_dXs = layer(padded2list(Xp), is_train)

    def backprop(dYp):
        return list2padded(get_dXs(padded2list(dYp)))

    return list2padded(Ys), backprop
