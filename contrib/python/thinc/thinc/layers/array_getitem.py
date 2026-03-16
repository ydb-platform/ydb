from typing import Sequence, Tuple, TypeVar, Union

from ..model import Model
from ..types import ArrayXd, FloatsXd, IntsXd

AxisIndex = Union[int, slice, Sequence[int]]
Index = Union[AxisIndex, Tuple[AxisIndex, ...]]
ArrayTXd = TypeVar("ArrayTXd", bound=ArrayXd)


def array_getitem(index: Index) -> Model[ArrayTXd, ArrayTXd]:
    """Index into input arrays, and return the subarrays.

    index:
        A valid numpy-style index. Multi-dimensional indexing can be performed
        by passing in a tuple, and slicing can be performed using the slice object.
        For instance, X[:, :-1] would be (slice(None, None), slice(None, -1)).
    """
    return Model("array-getitem", forward, attrs={"index": index})


def floats_getitem(index: Index) -> Model[FloatsXd, FloatsXd]:
    """Index into input arrays, and return the subarrays.

    This delegates to `array_getitem`, but allows type declarations.
    """
    return Model("floats-getitem", forward, attrs={"index": index})


def ints_getitem(index: Index) -> Model[IntsXd, IntsXd]:
    """Index into input arrays, and return the subarrays.

    This delegates to `array_getitem`, but allows type declarations.
    """
    return Model("ints-getitem", forward, attrs={"index": index})


def forward(model, X, is_train):
    index = model.attrs["index"]
    shape = X.shape
    dtype = X.dtype

    def backprop_get_column(dY):
        dX = model.ops.alloc(shape, dtype=dtype)
        dX[index] = dY
        return dX

    if len(X) == 0:
        return X, backprop_get_column
    Y = X[index]
    return Y, backprop_get_column
