from typing import Any, Callable, Dict, Optional, Tuple, TypeVar, Union, cast

from ..config import registry
from ..initializers import uniform_init
from ..model import Model
from ..types import Floats1d, Floats2d, Ints1d, Ints2d
from ..util import partial
from .array_getitem import ints_getitem
from .chain import chain

InT = TypeVar("InT", bound=Union[Ints1d, Ints2d])
OutT = Floats2d


@registry.layers("HashEmbed.v1")
def HashEmbed(
    nO: int,
    nV: int,
    *,
    seed: Optional[int] = None,
    column: Optional[int] = None,
    initializer: Optional[Callable] = None,
    dropout: Optional[float] = None
) -> Model[InT, OutT]:
    """
    An embedding layer that uses the “hashing trick” to map keys to distinct values.
    The hashing trick involves hashing each key four times with distinct seeds,
    to produce four likely differing values. Those values are modded into the
    table, and the resulting vectors summed to produce a single result. Because
    it’s unlikely that two different keys will collide on all four “buckets”,
    most distinct keys will receive a distinct vector under this scheme, even
    when the number of vectors in the table is very low.
    """
    attrs: Dict[str, Any] = {"column": column, "seed": seed}
    if initializer is None:
        initializer = uniform_init
    if dropout is not None:
        attrs["dropout_rate"] = dropout
    model: Model = Model(
        "hashembed",
        forward,
        init=partial(init, initializer),
        params={"E": None},
        dims={"nO": nO, "nV": nV, "nI": None},
        attrs=attrs,
    )
    if seed is None:
        model.attrs["seed"] = model.id
    if column is not None:
        # This is equivalent to array[:, column]. What you're actually doing
        # there is passing in a tuple: array[(:, column)], except in the context
        # of array indexing, the ":" creates an object slice(0, None).
        # So array[:, column] is array.__getitem__(slice(0), column).
        model = chain(ints_getitem((slice(0, None), column)), model)
    model.attrs["column"] = column
    return cast(Model[InT, OutT], model)


def forward(
    model: Model[Ints1d, OutT], ids: Ints1d, is_train: bool
) -> Tuple[OutT, Callable]:
    vectors = cast(Floats2d, model.get_param("E"))
    nV = vectors.shape[0]
    nO = vectors.shape[1]
    if len(ids) == 0:
        output: Floats2d = model.ops.alloc2f(0, nO, dtype=vectors.dtype)
    else:
        ids = model.ops.as_contig(ids, dtype="uint64")
        nN = ids.shape[0]
        seed: int = model.attrs["seed"]
        keys = model.ops.hash(ids, seed) % nV
        output = model.ops.gather_add(vectors, keys)
        drop_mask = None
        if is_train:
            dropout: Optional[float] = model.attrs.get("dropout_rate")
            drop_mask = cast(Floats1d, model.ops.get_dropout_mask((nO,), dropout))
            if drop_mask is not None:
                output *= drop_mask

    def backprop(d_vectors: OutT) -> Ints1d:
        if drop_mask is not None:
            d_vectors *= drop_mask
        dE = model.ops.alloc2f(*vectors.shape)
        keysT = model.ops.as_contig(keys.T, dtype="i")
        for i in range(keysT.shape[0]):
            model.ops.scatter_add(dE, keysT[i], d_vectors)
        model.inc_grad("E", dE)
        dX = model.ops.alloc1i(nN)
        return dX

    return output, backprop


def init(
    initializer: Callable,
    model: Model[Ints1d, OutT],
    X: Optional[Ints1d] = None,
    Y: Optional[OutT] = None,
) -> None:
    E = initializer(model.ops, (model.get_dim("nV"), model.get_dim("nO")))
    model.set_param("E", E)
