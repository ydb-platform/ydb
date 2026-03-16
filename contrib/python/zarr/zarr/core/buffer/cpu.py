from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
)

import numpy as np
import numpy.typing as npt

from zarr.core.buffer import core
from zarr.registry import (
    register_buffer,
    register_ndbuffer,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import Self

    from zarr.core.buffer.core import ArrayLike, NDArrayLike
    from zarr.core.common import BytesLike


class Buffer(core.Buffer):
    """A flat contiguous memory block

    We use Buffer throughout Zarr to represent a contiguous block of memory.

    A Buffer is backed by a underlying array-like instance that represents
    the memory. The memory type is unspecified; can be regular host memory,
    CUDA device memory, or something else. The only requirement is that the
    array-like instance can be copied/converted to a regular Numpy array
    (host memory).

    Notes
    -----
    This buffer is untyped, so all indexing and sizes are in bytes.

    Parameters
    ----------
    array_like
        array-like object that must be 1-dim, contiguous, and byte dtype.
    """

    def __init__(self, array_like: ArrayLike) -> None:
        super().__init__(array_like)

    @classmethod
    def create_zero_length(cls) -> Self:
        return cls(np.array([], dtype="B"))

    @classmethod
    def from_buffer(cls, buffer: core.Buffer) -> Self:
        """Create a new buffer of an existing Buffer

        This is useful if you want to ensure that an existing buffer is
        of the correct subclass of Buffer. E.g., MemoryStore uses this
        to return a buffer instance of the subclass specified by its
        BufferPrototype argument.

        Typically, this only copies data if the data has to be moved between
        memory types, such as from host to device memory.

        Parameters
        ----------
        buffer
            buffer object.

        Returns
        -------
            A new buffer representing the content of the input buffer

        Notes
        -----
        Subclasses of `Buffer` must override this method to implement
        more optimal conversions that avoid copies where possible
        """
        return cls.from_array_like(buffer.as_numpy_array())

    @classmethod
    def from_bytes(cls, bytes_like: BytesLike) -> Self:
        """Create a new buffer of a bytes-like object (host memory)

        Parameters
        ----------
        bytes_like
            bytes-like object

        Returns
        -------
            New buffer representing `bytes_like`
        """
        return cls.from_array_like(np.frombuffer(bytes_like, dtype="B"))

    def as_numpy_array(self) -> npt.NDArray[Any]:
        """Returns the buffer as a NumPy array (host memory).

        Notes
        -----
        Might have to copy data, consider using `.as_array_like()` instead.

        Returns
        -------
            NumPy array of this buffer (might be a data copy)
        """
        return np.asanyarray(self._data)

    def combine(self, others: Iterable[core.Buffer]) -> Self:
        data = [np.asanyarray(self._data)]
        for buf in others:
            other_array = buf.as_array_like()
            assert other_array.dtype == np.dtype("B")
            data.append(np.asanyarray(other_array))
        return self.__class__(np.concatenate(data))


class NDBuffer(core.NDBuffer):
    """An n-dimensional memory block

    We use NDBuffer throughout Zarr to represent a n-dimensional memory block.

    A NDBuffer is backed by a underlying ndarray-like instance that represents
    the memory. The memory type is unspecified; can be regular host memory,
    CUDA device memory, or something else. The only requirement is that the
    ndarray-like instance can be copied/converted to a regular Numpy array
    (host memory).

    Notes
    -----
    The two buffer classes Buffer and NDBuffer are very similar. In fact, Buffer
    is a special case of NDBuffer where dim=1, stride=1, and dtype="B". However,
    in order to use Python's type system to differentiate between the contiguous
    Buffer and the n-dim (non-contiguous) NDBuffer, we keep the definition of the
    two classes separate.

    Parameters
    ----------
    array
        ndarray-like object that is convertible to a regular Numpy array.
    """

    def __init__(self, array: NDArrayLike) -> None:
        super().__init__(array)

    @classmethod
    def create(
        cls,
        *,
        shape: Iterable[int],
        dtype: npt.DTypeLike,
        order: Literal["C", "F"] = "C",
        fill_value: Any | None = None,
    ) -> Self:
        # np.zeros is much faster than np.full, and therefore using it when possible is better.
        if fill_value is None or (isinstance(fill_value, int) and fill_value == 0):
            return cls(np.zeros(shape=tuple(shape), dtype=dtype, order=order))
        else:
            return cls(np.full(shape=tuple(shape), fill_value=fill_value, dtype=dtype, order=order))

    @classmethod
    def empty(
        cls, shape: tuple[int, ...], dtype: npt.DTypeLike, order: Literal["C", "F"] = "C"
    ) -> Self:
        return cls(np.empty(shape=shape, dtype=dtype, order=order))

    @classmethod
    def from_numpy_array(cls, array_like: npt.ArrayLike) -> Self:
        return cls.from_ndarray_like(np.asanyarray(array_like))

    def as_numpy_array(self) -> npt.NDArray[Any]:
        """Returns the buffer as a NumPy array (host memory).

        Warnings
        --------
        Might have to copy data, consider using `.as_ndarray_like()` instead.

        Returns
        -------
            NumPy array of this buffer (might be a data copy)
        """
        return np.asanyarray(self._data)

    def __getitem__(self, key: Any) -> Self:
        return self.__class__(np.asanyarray(self._data.__getitem__(key)))

    def __setitem__(self, key: Any, value: Any) -> None:
        if isinstance(value, NDBuffer):
            value = value._data
        self._data.__setitem__(key, value)


def as_numpy_array_wrapper(
    func: Callable[[npt.NDArray[Any]], bytes], buf: core.Buffer, prototype: core.BufferPrototype
) -> core.Buffer:
    """Converts the input of `func` to a numpy array and the output back to `Buffer`.

    This function is useful when calling a `func` that only support host memory such
    as `GZip.decode` and `Blosc.decode`. In this case, use this wrapper to convert
    the input `buf` to a Numpy array and convert the result back into a `Buffer`.

    Parameters
    ----------
    func
        The callable that will be called with the converted `buf` as input.
        `func` must return bytes, which will be converted into a `Buffer`
        before returned.
    buf
        The buffer that will be converted to a Numpy array before given as
        input to `func`.
    prototype
        The prototype of the output buffer.

    Returns
    -------
        The result of `func` converted to a `Buffer`
    """
    return prototype.buffer.from_bytes(func(buf.as_numpy_array()))


# CPU buffer prototype using numpy arrays
buffer_prototype = core.BufferPrototype(buffer=Buffer, nd_buffer=NDBuffer)
# default_buffer_prototype = buffer_prototype


# The numpy prototype used for E.g. when reading the shard index
def numpy_buffer_prototype() -> core.BufferPrototype:
    return core.BufferPrototype(buffer=Buffer, nd_buffer=NDBuffer)


register_buffer(Buffer, qualname="zarr.buffer.cpu.Buffer")
register_ndbuffer(NDBuffer, qualname="zarr.buffer.cpu.NDBuffer")


# backwards compatibility
register_buffer(Buffer, qualname="zarr.core.buffer.cpu.Buffer")
register_ndbuffer(NDBuffer, qualname="zarr.core.buffer.cpu.NDBuffer")
