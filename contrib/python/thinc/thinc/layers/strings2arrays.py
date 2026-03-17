from ctypes import c_uint64
from typing import Callable, List, Sequence, Tuple

from murmurhash import hash_unicode

from ..config import registry
from ..model import Model
from ..types import Ints2d

InT = Sequence[Sequence[str]]
OutT = List[Ints2d]


@registry.layers("strings2arrays.v1")
def strings2arrays() -> Model[InT, OutT]:
    """Transform a sequence of string sequences to a list of arrays."""
    return Model("strings2arrays", forward)


def forward(model: Model[InT, OutT], Xs: InT, is_train: bool) -> Tuple[OutT, Callable]:
    # Cast 32-bit (signed) integer to 64-bit unsigned, since such casting
    # is deprecated in NumPy.
    hashes = [[c_uint64(hash_unicode(word)).value for word in X] for X in Xs]
    hash_arrays = [model.ops.asarray1i(h, dtype="uint64") for h in hashes]
    arrays = [model.ops.reshape2i(array, -1, 1) for array in hash_arrays]

    def backprop(dX: OutT) -> InT:
        return []

    return arrays, backprop
