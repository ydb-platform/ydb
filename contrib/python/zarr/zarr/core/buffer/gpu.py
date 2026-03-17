from __future__ import annotations

import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    cast,
)

import numpy as np
import numpy.typing as npt

from zarr.core.buffer import core
from zarr.core.buffer.core import ArrayLike, BufferPrototype, NDArrayLike
from zarr.errors import ZarrUserWarning
from zarr.registry import (
    register_buffer,
    register_ndbuffer,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import Self

    from zarr.core.common import BytesLike

try:
    import cupy as cp
except ImportError:
    cp = None


class Buffer(core.Buffer):
    """A flat contiguous memory block on the GPU

    We use Buffer throughout Zarr to represent a contiguous block of memory.

    A Buffer is backed by an underlying array-like instance that represents
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
        if cp is None:
            raise ImportError(
                "Cannot use zarr.buffer.gpu.Buffer without cupy. Please install cupy."
            )

        if array_like.ndim != 1:
            raise ValueError("array_like: only 1-dim allowed")
        if array_like.dtype != np.dtype("B"):
            raise ValueError("array_like: only byte dtype allowed")

        if not hasattr(array_like, "__cuda_array_interface__"):
            # Slow copy based path for arrays that don't support the __cuda_array_interface__
            # TODO: Add a fast zero-copy path for arrays that support the dlpack protocol
            msg = (
                "Creating a zarr.buffer.gpu.Buffer with an array that does not support the "
                "__cuda_array_interface__ for zero-copy transfers, "
                "falling back to slow copy based path"
            )
            warnings.warn(
                msg,
                category=ZarrUserWarning,
                stacklevel=2,
            )
        self._data = cp.asarray(array_like)

    @classmethod
    def create_zero_length(cls) -> Self:
        """Create an empty buffer with length zero

        Returns
        -------
            New empty 0-length buffer
        """
        return cls(cp.array([], dtype="B"))

    @classmethod
    def from_buffer(cls, buffer: core.Buffer) -> Self:
        """Create a GPU Buffer given an arbitrary Buffer
        This will try to be zero-copy if `buffer` is already on the
        GPU and will trigger a copy if not.

        Returns
        -------
            New GPU Buffer constructed from `buffer`
        """
        return cls(buffer.as_array_like())

    @classmethod
    def from_bytes(cls, bytes_like: BytesLike) -> Self:
        return cls.from_array_like(cp.frombuffer(bytes_like, dtype="B"))

    def as_numpy_array(self) -> npt.NDArray[Any]:
        return cast("npt.NDArray[Any]", cp.asnumpy(self._data))

    def combine(self, others: Iterable[core.Buffer]) -> Self:
        data = [cp.asanyarray(self._data)]
        for other in others:
            other_array = other.as_array_like()
            assert other_array.dtype == np.dtype("B")
            gpu_other = Buffer(other_array)
            gpu_other_array = gpu_other.as_array_like()
            data.append(cp.asanyarray(gpu_other_array))
        return self.__class__(cp.concatenate(data))


class NDBuffer(core.NDBuffer):
    """A n-dimensional memory block on the GPU

    We use NDBuffer throughout Zarr to represent a n-dimensional memory block.

    A NDBuffer is backed by an underlying ndarray-like instance that represents
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
        if cp is None:
            raise ImportError(
                "Cannot use zarr.buffer.gpu.NDBuffer without cupy. Please install cupy."
            )

        # assert array.ndim > 0
        assert array.dtype != object
        self._data = array

        if not hasattr(array, "__cuda_array_interface__"):
            # Slow copy based path for arrays that don't support the __cuda_array_interface__
            # TODO: Add a fast zero-copy path for arrays that support the dlpack protocol
            msg = (
                "Creating a zarr.buffer.gpu.NDBuffer with an array that does not support the "
                "__cuda_array_interface__ for zero-copy transfers, "
                "falling back to slow copy based path"
            )
            warnings.warn(
                msg,
                stacklevel=2,
            )
        self._data = cp.asarray(array)

    @classmethod
    def create(
        cls,
        *,
        shape: Iterable[int],
        dtype: npt.DTypeLike,
        order: Literal["C", "F"] = "C",
        fill_value: Any | None = None,
    ) -> Self:
        ret = cls(cp.empty(shape=tuple(shape), dtype=dtype, order=order))
        if fill_value is not None:
            ret.fill(fill_value)
        return ret

    @classmethod
    def empty(
        cls, shape: tuple[int, ...], dtype: npt.DTypeLike, order: Literal["C", "F"] = "C"
    ) -> Self:
        return cls(cp.empty(shape=shape, dtype=dtype, order=order))

    @classmethod
    def from_numpy_array(cls, array_like: npt.ArrayLike) -> Self:
        """Create a new buffer of Numpy array-like object

        Parameters
        ----------
        array_like
            Object that can be coerced into a Numpy array

        Returns
        -------
            New buffer representing `array_like`
        """
        return cls(cp.asarray(array_like))

    def as_numpy_array(self) -> npt.NDArray[Any]:
        """Returns the buffer as a NumPy array (host memory).

        Warnings
        --------
        Might have to copy data, consider using `.as_ndarray_like()` instead.

        Returns
        -------
            NumPy array of this buffer (might be a data copy)
        """
        return cast("npt.NDArray[Any]", cp.asnumpy(self._data))

    def __getitem__(self, key: Any) -> Self:
        return self.__class__(self._data.__getitem__(key))

    def __setitem__(self, key: Any, value: Any) -> None:
        if isinstance(value, NDBuffer):
            value = value._data
        elif isinstance(value, core.NDBuffer):
            gpu_value = NDBuffer(value.as_ndarray_like())
            value = gpu_value._data
        self._data.__setitem__(key, value)


buffer_prototype = BufferPrototype(buffer=Buffer, nd_buffer=NDBuffer)

register_buffer(Buffer, qualname="zarr.buffer.gpu.Buffer")
register_ndbuffer(NDBuffer, qualname="zarr.buffer.gpu.NDBuffer")

# backwards compatibility
register_buffer(Buffer, qualname="zarr.core.buffer.gpu.Buffer")
register_ndbuffer(NDBuffer, qualname="zarr.core.buffer.gpu.NDBuffer")
