from typing import Callable, List, Optional, Tuple, TypeVar, Union, cast

from ..backends import NumpyOps
from ..config import registry
from ..model import Model
from ..types import Array2d, Ints1d, List2d, ListXd, Padded, Ragged

NUMPY_OPS = NumpyOps()


RaggedData = Tuple[Array2d, Ints1d]
SeqT = TypeVar("SeqT", bound=Union[Padded, Ragged, ListXd, RaggedData])


@registry.layers("with_ragged.v1")
def with_ragged(layer: Model[Ragged, Ragged]) -> Model[SeqT, SeqT]:
    return Model(f"with_ragged({layer.name})", forward, init=init, layers=[layer])


def forward(
    model: Model[SeqT, SeqT], Xseq: SeqT, is_train: bool
) -> Tuple[SeqT, Callable]:
    layer: Model[Ragged, Ragged] = model.layers[0]
    if isinstance(Xseq, Ragged):
        return cast(Tuple[SeqT, Callable], layer(Xseq, is_train))
    elif isinstance(Xseq, Padded):
        return cast(Tuple[SeqT, Callable], _padded_forward(layer, Xseq, is_train))
    elif _is_ragged_data(Xseq):
        return cast(
            Tuple[SeqT, Callable],
            _tuple_forward(layer, cast(RaggedData, Xseq), is_train),
        )
    else:
        return cast(
            Tuple[SeqT, Callable], _list_forward(layer, cast(List, Xseq), is_train)
        )


def init(
    model: Model[SeqT, SeqT],
    X: Optional[SeqT] = None,
    Y: Optional[SeqT] = None,
) -> None:
    model.layers[0].initialize(
        X=_get_ragged(model, X) if X is not None else None,
        Y=_get_ragged(model, Y) if Y is not None else None,
    )


def _is_ragged_data(seq):
    return isinstance(seq, tuple) and len(seq) == 2


def _get_ragged(model: Model[SeqT, SeqT], seq: SeqT) -> Ragged:
    if isinstance(seq, Ragged):
        return seq
    elif isinstance(seq, Padded):
        lists = model.ops.padded2list(seq)
        lengths = model.ops.asarray1i([len(x) for x in lists])
        k = model.ops.flatten(lists)
        return Ragged(model.ops.flatten(lists), lengths)
    elif _is_ragged_data(seq):
        return Ragged(*seq)  # type: ignore[misc]
    else:
        list2d_seq = cast(List2d, seq)
        lengths = model.ops.asarray1i([len(x) for x in list2d_seq])
        return Ragged(model.ops.flatten(list2d_seq), lengths)


def _tuple_forward(
    layer: Model[Ragged, Ragged], X: RaggedData, is_train: bool
) -> Tuple[RaggedData, Callable]:
    Yr, get_dXr = layer(Ragged(*X), is_train)

    def backprop(dY: RaggedData) -> RaggedData:
        dXr = get_dXr(Ragged(*dY))
        return (dXr.data, dXr.lengths)

    return (Yr.data, Yr.lengths), backprop


def _padded_forward(
    layer: Model[Ragged, Ragged], Xp: Padded, is_train: bool
) -> Tuple[Padded, Callable]:
    # Assign these to locals, to keep code a bit shorter.
    list2padded = layer.ops.list2padded
    padded2list = layer.ops.padded2list
    unflatten = layer.ops.unflatten
    flatten = layer.ops.flatten
    # It's worth being a bit careful about memory here, as the activations
    # are potentially large on GPU. So we make nested function calls instead
    # of assigning to temporaries where possible, so memory can be reclaimed
    # sooner.
    Xs = padded2list(Xp)
    # Bit annoying here: padded is in a different order, so we need to make new
    # lengths. The lengths are unconditionally allocated in CPU memory, because
    # otherwire unflatten would move GPU allocations to the CPU again. For the
    # ragged arrays we let the layer's ops determine how lengths should be
    # stored to ensure that the array and lengths use the same type of memory.
    lengths = NUMPY_OPS.asarray1i([len(x) for x in Xs])
    Yr, get_dXr = layer(Ragged(flatten(Xs), layer.ops.asarray1i(lengths)), is_train)

    def backprop(dYp: Padded):
        flattened = flatten(padded2list(dYp))
        dXr = get_dXr(Ragged(flattened, lengths))
        return list2padded(unflatten(dXr.data, lengths))

    return (
        list2padded(unflatten(Yr.data, Yr.lengths)),
        backprop,
    )


def _list_forward(
    layer: Model[Ragged, Ragged], Xs: List, is_train: bool
) -> Tuple[List, Callable]:
    # Assign these to locals, to keep code a bit shorter.
    flatten = layer.ops.flatten
    unflatten = layer.ops.unflatten

    lengths = [len(x) for x in Xs]
    Yr, get_dXr = layer(Ragged(flatten(Xs), layer.ops.asarray1i(lengths)), is_train)

    def backprop(dYs):
        flattened = flatten(dYs)
        return unflatten(get_dXr(Ragged(flattened, lengths)).data, lengths)

    return unflatten(Yr.data, Yr.lengths), backprop
