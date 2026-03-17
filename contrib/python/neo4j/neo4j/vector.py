# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Vector type to be exchanged with the DBMS."""

from __future__ import annotations as _

import abc as _abc
import struct as _struct
import sys as _sys
from enum import Enum as _Enum

from . import _typing as _t


if _t.TYPE_CHECKING:
    # "Why?", I hear you ask. Because sphinx of course.
    # This beautiful construct helps sphinx to properly resolve the type hints.
    import numpy as _np
    import pyarrow as _pa
else:
    from ._optional_deps import (
        np as _np,
        pa as _pa,
    )


if False:
    # Ugly work-around to make sphinx understand `@_t.overload`
    import typing as _t  # type: ignore[no-redef]


try:
    from ._rust.vector import swap_endian as _swap_endian_unchecked_rust
except ImportError:
    _swap_endian_unchecked_rust = None


__all__ = [
    "Vector",
    "VectorDType",
    "VectorEndian",
]


_DEFAULT = object()


class Vector:
    r"""
    A class representing a Neo4j vector.

    The constructor accepts various types of data to create a vector.
    Depending on ``data``'s type, further arguments may be required/allowed.
    Examples of valid invocations are::

        Vector([1, 2, 3], "i8")
        Vector(b"\x00\x01\x00\x02", VectorDType.I16)
        Vector(b"\x01\x00\x02\x00", "i16", byteorder="little")
        Vector(numpy.array([1, 2, 3]))
        Vector(pyarrow.array([1, 2, 3]))

    Internally, a vector is stored as a contiguous block of memory
    (:class:`bytes`), containing homogeneous values encoded in big-endian
    order. Support for this feature requires a DBMS supporting Bolt version
    6.0 or later.

    :param data:
        The data from which the vector will be constructed.
        The constructor accepts the following types:

        * ``Iterable[float]``, ``Iterable[int]`` (but not ``bytes`` or
          ``bytearray``):
          Use an iterable of floats or an iterable of ints to construct the
          vector from native Python values.
          The ``dtype`` parameter is required.
          See also: :meth:`.from_native`.
        * ``bytes``, ``bytearray``: Use raw bytes to construct the vector.
          The ``dtype`` parameter is required and ``byteorder`` is optional.
        * ``numpy.ndarray``: Use a numpy array to construct the vector.
          No further parameters are accepted.
          See also: :meth:`.from_numpy`.
        * ``pyarrow.Array``: Use a pyarrow array to construct the vector.
          No further parameters are accepted.
          See also: :meth:`.from_pyarrow`.
    :param dtype: The type of the vector.
        See :class:`.VectorDType` for currently supported inner data types.
        See also :attr:`.dtype`.

        This parameter is required if ``data`` is of type :class:`bytes`,
        :class:`bytearray`, ``Iterable[float]``, or ``Iterable[int]``.
        Otherwise, it must be omitted.
    :param byteorder: The endianness of the input data (default: ``"big"``).
        If ``"little"`` is given, ``neo4j-rust-ext`` or ``numpy`` is used to
        speed up the internal byte flipping (if either package is installed).
        Use :data:`sys.byteorder` if you want to use the system's native
        endianness.

        This parameter is optional if ``data`` is of type ``bytes`` or
        ``bytearray``. Otherwise, it must be omitted.

    :raises ValueError:
        Depending on the type of ``data``:
            * ``Iterable[float]``, ``Iterable[int]`` (excluding byte types):
                * If the dtype is not supported.
            * ``bytes``, ``bytearray``:
                * If the dtype is not supported or data's size is not a
                  multiple of dtype's size.
                * If byteorder is not one of ``"big"`` or ``"little"``.
            * ``numpy.ndarray``:
                * If the dtype is not supported.
                * If the array is not one-dimensional.
            * ``pyarrow.Array``:
                * If the array's type is not supported.
                * If the array contains null values.
    :raises TypeError:
        Depending on the type of ``data``:
            * ``Iterable[float]``, ``Iterable[int]`` (excluding byte types):
                * If data's elements don't match the expected type depending on
                  dtype.
    :raises OverflowError:
        Depending on the type of ``data``:
            * ``Iterable[float]``, ``Iterable[int]`` (excluding byte types):
                * If the value is out of range for the given type.

    .. versionadded: 6.0
    """

    __slots__ = ("__weakref__", "_inner")

    _inner: _InnerVector

    @_t.overload
    def __init__(
        self,
        data: _t.Iterable[float],
        dtype: _T_VectorDTypeFloat,
        /,
    ) -> None: ...

    @_t.overload
    def __init__(
        self,
        data: _t.Iterable[int],
        dtype: _T_VectorDTypeInt,
        /,
    ) -> None: ...

    @_t.overload
    def __init__(
        self,
        data: bytes | bytearray,
        dtype: _T_VectorDType,
        /,
        *,
        byteorder: _T_VectorEndian = "big",
    ) -> None: ...

    @_t.overload
    def __init__(self, data: _np.ndarray, /) -> None: ...

    @_t.overload
    def __init__(self, data: _pa.Array, /) -> None: ...

    def __init__(self, data, *args, **kwargs) -> None:
        if isinstance(data, (bytes, bytearray)):
            self._set_bytes(bytes(data), *args, **kwargs)
        elif _np is not None and isinstance(data, _np.ndarray):
            self._set_numpy(data, *args, **kwargs)
        elif _pa is not None and isinstance(data, _pa.Array):
            self._set_pyarrow(data, *args, **kwargs)
        else:
            self._set_native(data, *args, **kwargs)

    def raw(self, /, *, byteorder: _T_VectorEndian = "big") -> bytes:
        """
        Get the raw bytes of the vector.

        The data is a continuous block of memory, containing an array of the
        vector's data type. The data is stored in big-endian order. Pass
        another byte-order to this method to get the converted data.

        :param byteorder: The endianness the data should be returned in.
            If the data's byte-order needs flipping, this method tries to use
            ``neo4j-rust-ext`` or ``numpy``, if installed, to speed up the
            process. Use :data:`sys.byteorder` if you want to use the system's
            native endianness.

        :returns: The raw bytes of the vector.

        :raises ValueError:
            If byteorder is not one of ``"big"`` or ``"little"``.
        """
        match byteorder:
            case "big":
                return self._inner.data
            case "little":
                return self._inner.data_le
            case _:
                raise ValueError(
                    f"Invalid byteorder: {byteorder!r}. "
                    "Must be 'big' or 'little'."
                )

    def set_raw(
        self,
        data: bytes,
        /,
        *,
        byteorder: _T_VectorEndian = "big",
    ) -> None:
        """
        Set the raw bytes of the vector.

        :param data: The new raw bytes of the vector.
        :param byteorder: The endianness of ``data``.
            The data will always be stored in big-endian order. If passed-in
            byte-order needs flipping, this method tries to use
            ``neo4j-rust-ext`` or ``numpy``, if installed, to speed up the
            process. Use :data:`sys.byteorder` if you want to use the system's
            native endianness.

        :raises ValueError:
          * If data's size is not a multiple of dtype's size.
          * If byteorder is not one of ``"big"`` or ``"little"``.
        :raises TypeError: If the data is not of type bytes.
        """
        match byteorder:
            case "big":
                self._inner.data = data
            case "little":
                self._inner.data_le = data
            case _:
                raise ValueError(
                    f"Invalid byteorder: {byteorder!r}. "
                    "Must be 'big' or 'little'."
                )

    @property
    def dtype(self) -> VectorDType:
        """
        Get the type of the vector.

        :returns: The type of the vector.
        """
        return self._inner.dtype

    def __len__(self) -> int:
        """
        Get the number of elements in the vector.

        :returns: The number of elements in the vector.
        """
        return len(self._inner)

    def __str__(self) -> str:
        return str(self._inner)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.raw()!r}, {self.dtype.value!r})"
        )

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
        dtype: _T_VectorDType,
        /,
        *,
        byteorder: _T_VectorEndian = "big",
    ) -> _t.Self:
        """
        Create a Vector instance from raw bytes.

        :param data: The raw bytes to create the vector from.
        :param dtype: The type of the vector.
            See also :attr:`.dtype`.
        :param byteorder: The endianness of the data.
            If ``"little"``, the bytes in data will be flipped to big-endian.
            If installed, ``neo4j-rust-ext`` or ``numpy`` will be used to speed
            up the byte flipping. Use :data:`sys.byteorder` if you want to use
            the system's native endianness.

        :raises ValueError:
          * If data's size is not a multiple of dtype's size.
          * If byteorder is not one of ``"big"`` or ``"little"``.
        :raises TypeError: If the data is not of type bytes.
        """
        obj = cls.__new__(cls)
        obj._set_bytes(data, dtype, byteorder=byteorder)
        return obj

    def _set_bytes(
        self,
        data: bytes,
        dtype: _T_VectorDType,
        /,
        *,
        byteorder: _T_VectorEndian = "big",
    ) -> None:
        self._inner = _get_type(dtype)(data, byteorder=byteorder)

    @classmethod
    @_t.overload
    def from_native(
        cls, data: _t.Iterable[float], dtype: _T_VectorDTypeFloat, /
    ) -> _t.Self: ...

    @classmethod
    @_t.overload
    def from_native(
        cls, data: _t.Iterable[int], dtype: _T_VectorDTypeInt, /
    ) -> _t.Self: ...

    @classmethod
    def from_native(
        cls,
        data: _t.Iterable[float] | _t.Iterable[int],
        dtype: _T_VectorDType,
        /,
    ) -> _t.Self:
        """
        Create a Vector instance from an iterable of values.

        :param data: The list, tuple, or other iterable of values to create the
            vector from.
        :param dtype: The type of the vector.
            See also :attr:`.dtype`.

        ``data`` must contain values that match the expected type given by
        ``dtype``:

        * ``dtype == "f32"``: :class:`float`
        * ``dtype == "f64"``: :class:`float`
        * ``dtype == "i8"``: :class:`int`
        * ``dtype == "i16"``: :class:`int`
        * ``dtype == "i32"``: :class:`int`
        * ``dtype == "i64"``: :class:`int`

        :raises ValueError: If the dtype is not supported.
        :raises TypeError: If data's elements don't match the expected type
            depending on dtype.
        :raises OverflowError: If the value is out of range for the given type.
        """
        obj = cls.__new__(cls)
        obj._set_native(data, dtype)
        return obj

    def _set_native(
        self,
        data: _t.Iterable[float] | _t.Iterable[int],
        dtype: _T_VectorDType,
        /,
    ) -> None:
        self._inner = _get_type(dtype).from_native(data)

    def to_native(self) -> list[object]:
        """
        Convert the vector to a native Python list.

        The type of the elements in the list depends on the dtype of the
        vector. See :meth:`Vector.from_native` for details.

        :returns: A list of values representing the vector.
        """
        return self._inner.to_native()

    @classmethod
    def from_numpy(cls, data: _np.ndarray, /) -> _t.Self:
        """
        Create a Vector instance from a numpy array.

        :param data: The numpy array to create the vector from.
            The array must be one-dimensional and have a dtype that is
            supported by Neo4j vectors: ``float64``, ``float32``,
            ``int64``, ``int32``, ``int16``, or ``int8``.
            See also :class:`.VectorDType`.

        :raises ValueError:
          * If the dtype is not supported.
          * If the array is not one-dimensional.
        :raises ImportError: If numpy is not installed.

        :returns: A Vector instance constructed from the numpy array.
        """
        obj = cls.__new__(cls)
        obj._set_numpy(data)
        return obj

    def to_numpy(self) -> _np.ndarray:
        """
        Convert the vector to a numpy array.

        The array's dtype depends on the dtype of the vector. However, it will
        always be in big-endian order.

        :returns: A numpy array representing the vector.

        :raises ImportError: If numpy is not installed.
        """
        return self._inner.to_numpy()

    def _set_numpy(self, data: _np.ndarray, /) -> None:
        if data.ndim != 1:
            raise ValueError("Data must be one-dimensional")
        type_: type[_InnerVector]
        match data.dtype.name:
            case "float64":
                type_ = _VecF64
            case "float32":
                type_ = _VecF32
            case "int64":
                type_ = _VecI64
            case "int32":
                type_ = _VecI32
            case "int16":
                type_ = _VecI16
            case "int8":
                type_ = _VecI8
            case _:
                raise ValueError(f"Unsupported numpy dtype: {data.dtype.name}")
        self._inner = type_.from_numpy(data)

    @classmethod
    def from_pyarrow(cls, data: _pa.Array, /) -> _t.Self:
        """
        Create a Vector instance from a pyarrow array.

        PyArrow stores data in little endian. Therefore, the byte-order needs
        to be swapped. If ``neo4j-rust-ext`` or ``numpy`` is installed, it will
        be used to speed up the byte flipping.

        :param data: The pyarrow array to create the vector from.
            The array must have a type that is supported by Neo4j.
            See also :class:`.VectorDType`.
        :raises ValueError:
          * If the array's type is not supported.
          * If the array contains null values.
        :raises ImportError: If pyarrow is not installed.

        :returns: A Vector instance constructed from the pyarrow array.
        """
        obj = cls.__new__(cls)
        obj._set_pyarrow(data)
        return obj

    def to_pyarrow(self) -> _pa.Array:
        """
        Convert the vector to a pyarrow array.

        :returns: A pyarrow array representing the vector.

        :raises ImportError: If pyarrow is not installed.
        """
        return self._inner.to_pyarrow()

    def _set_pyarrow(self, data: _pa.Array, /) -> None:
        import pyarrow

        type_: type[_InnerVector]
        if data.type == pyarrow.float64():
            type_ = _VecF64
        elif data.type == pyarrow.float32():
            type_ = _VecF32
        elif data.type == pyarrow.int64():
            type_ = _VecI64
        elif data.type == pyarrow.int32():
            type_ = _VecI32
        elif data.type == pyarrow.int16():
            type_ = _VecI16
        elif data.type == pyarrow.int8():
            type_ = _VecI8
        else:
            raise ValueError(f"Unsupported pyarrow dtype: {data.type}")
        inner = type_.from_pyarrow(data)
        self._inner = inner

    # TODO: consider conversion to/from
    #   * tensorflow
    #   * pandas
    #   * polars


class VectorEndian(str, _Enum):
    """
    Data endianness (i.e., byte order) of the elements in a :class:`Vector`.

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.VectorEndian` value will also accept
    a string::

        >>> VectorEndian.BIG == "big"
        True
        >>> VectorEndian.LITTLE == "little"
        True

    .. seealso:: :attr:`Vector.raw`

    .. versionadded:: 6.0
    """

    BIG = "big"
    LITTLE = "little"


_T_VectorEndian = VectorEndian | _t.Literal["big", "little"]


class VectorDType(str, _Enum):
    """
    The data type of the elements in a :class:`Vector`.

    Currently supported types are:

        * ``f32``: 32-bit floating point number (single)
        * ``f64``: 64-bit floating point number (double)
        * ``i8``: 8-bit integer
        * ``i16``: 16-bit integer
        * ``i32``: 32-bit integer
        * ``i64``: 64-bit integer

    Inherits from :class:`str` and :class:`enum.Enum`.
    Every driver API accepting a :class:`.VectorDType` value will also accept
    a string::

        >>> VectorDType.F32 == "f32"
        True
        >>> VectorDType.I8 == "i8"
        True

    .. seealso:: :attr:`Vector.dtype`

    .. versionadded:: 6.0
    """

    F32 = "f32"
    F64 = "f64"
    I8 = "i8"
    I16 = "i16"
    I32 = "i32"
    I64 = "i64"


_T_VectorDType = (
    VectorDType | _t.Literal["f32", "f64", "i8", "i16", "i32", "i64"]
)
_T_VectorDTypeInt = _t.Literal[
    VectorDType.I8,
    VectorDType.I16,
    VectorDType.I32,
    VectorDType.I64,
    "i8",
    "i16",
    "i32",
    "i64",
]
_T_VectorDTypeFloat = _t.Literal[
    VectorDType.F32, VectorDType.F64, "f32", "f64"
]


def _swap_endian(type_size: int, data: bytes, /) -> bytes:
    """Swap from big endian to little endian."""
    if type_size == 1:
        return data
    if type_size not in {2, 4, 8}:
        raise ValueError(f"Unsupported type size: {type_size}")
    if len(data) % type_size != 0:
        raise ValueError(
            f"Data length {len(data)} is not a multiple of {type_size}"
        )
    return _swap_endian_unchecked(type_size, data)


def _swap_endian_unchecked_np(type_size: int, data: bytes, /) -> bytes:
    dtype: _np.dtype
    match type_size:
        case 2:
            dtype = _np.dtype("<i2")
        case 4:
            dtype = _np.dtype("<i4")
        case 8:
            dtype = _np.dtype("<i8")
        case _:
            raise ValueError(f"Unsupported type size: {type_size}")
    return _np.frombuffer(data, dtype=dtype).byteswap().tobytes()


def _swap_endian_unchecked_py(type_size: int, data: bytes, /) -> bytes:
    match type_size:
        case 2:
            fmt = "h"
        case 4:
            fmt = "i"
        case 8:
            fmt = "q"
        case _:
            raise ValueError(f"Unsupported type size: {type_size}")
    count = len(data) // type_size
    fmt_be = f">{count}{fmt}"
    fmt_le = f"<{count}{fmt}"
    return _struct.pack(fmt_be, *_struct.unpack(fmt_le, data))


if _swap_endian_unchecked_rust is not None:
    _swap_endian_unchecked = _swap_endian_unchecked_rust
elif _np is not None:
    _swap_endian_unchecked = _swap_endian_unchecked_np
else:
    _swap_endian_unchecked = _swap_endian_unchecked_py


def _get_type(dtype: _T_VectorDType, /) -> type[_InnerVector]:
    if isinstance(dtype, str):
        if dtype not in VectorDType.__members__.values():
            raise ValueError(f"Unsupported vector type: {dtype!r}.")
        dtype = VectorDType(dtype)
    if not isinstance(dtype, VectorDType):
        raise TypeError(f"Expected a VectorDType or str, got {type(dtype)}.")
    if dtype not in _TYPES:
        raise ValueError(f"Unsupported vector type: {dtype!r}.")
    return _TYPES[dtype]


_TYPES: dict[VectorDType, type[_InnerVector]] = {}


class _InnerVector(_abc.ABC):
    __slots__ = ("_data", "_data_le")

    dtype: _t.ClassVar[VectorDType]
    size: _t.ClassVar[int]
    cypher_inner_type_repr: _t.ClassVar[str]
    _data: bytes
    _data_le: bytes | None

    def __init__(
        self, data: bytes, /, *, byteorder: _T_VectorEndian = "big"
    ) -> None:
        super().__init__()
        if self.__class__ == _InnerVector:
            raise TypeError("Cannot instantiate abstract class InnerVector")
        match byteorder:
            case "big":
                self.data = data
                self._data_le = None
            case "little":
                self.data = _swap_endian(self.size, data)
                self._data_le = data
            case _:
                raise ValueError(
                    f"Invalid byteorder: {byteorder!r}. "
                    "Must be 'big' or 'little'."
                )

    @property
    def data(self) -> bytes:
        return self._data

    @data.setter
    def data(self, data: bytes, /) -> None:
        if not isinstance(data, bytes):
            raise TypeError("Data must be of type bytes")
        if not len(data) % self.size == 0:
            raise ValueError(
                f"Data length {len(data)} is not a multiple of {self.size}"
            )
        self._data = data

    @property
    def data_le(self) -> bytes:
        if self._data_le is None:
            self._data_le = _swap_endian(self.size, self.data)
        return self._data_le

    @data_le.setter
    def data_le(self, data: bytes, /) -> None:
        self.data = _swap_endian(self.size, data)
        self._data_le = data

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if _abc.ABC in cls.__bases__:
            return
        dtype = getattr(cls, "dtype", None)
        if not isinstance(dtype, VectorDType):
            raise TypeError(
                f"Class {cls.__name__} must have a VectorDType attribute"
                "'dtype'"
            )
        if not isinstance(getattr(cls, "size", None), int):
            raise TypeError(
                f"Class {cls.__name__} must have a str attribute 'size'"
            )
        if cls.size not in {1, 2, 4, 8}:
            # Either change the sub-type's size if it was a typo or add support
            # for the new size in the swap_endian function.
            raise ValueError(
                f"Class {cls.__name__} has an unhandled size {cls.size}"
            )
        if dtype in _TYPES:
            raise ValueError(
                f"Class {cls.__name__} has a duplicate type '{dtype}'"
            )
        _TYPES[dtype] = cls

    def __len__(self) -> int:
        return len(self.data) // self.size

    def __str__(self) -> str:
        size = len(self)
        type_repr = self.cypher_inner_type_repr
        values_repr = self._cypher_values_repr()
        return f"vector({values_repr}, {size}, {type_repr})"

    @_abc.abstractmethod
    def _cypher_values_repr(self) -> str: ...

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"{cls_name}({self.data!r})"

    @classmethod
    @_abc.abstractmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self: ...

    @_abc.abstractmethod
    def to_native(self) -> list[object]: ...

    @classmethod
    def from_numpy(cls, data: _np.ndarray, /) -> _t.Self:
        if data.dtype.byteorder == "<" or (
            data.dtype.byteorder == "=" and _sys.byteorder == "little"
        ):
            data = data.byteswap()
        return cls(data.tobytes())

    @_abc.abstractmethod
    def to_numpy(self) -> _np.ndarray: ...

    @classmethod
    def from_pyarrow(cls, data: _pa.Array, /) -> _t.Self:
        width = data.type.byte_width
        assert cls.size == width
        if _pa.compute.count(data, mode="only_null").as_py():
            raise ValueError("PyArrow array must not contain any null values.")
        _, buffer = data.buffers()
        buffer = buffer[
            data.offset * width : (data.offset + len(data)) * width
        ]
        return cls(bytes(buffer), byteorder=_sys.byteorder)

    @_abc.abstractmethod
    def to_pyarrow(self) -> _pa.Array: ...


class _InnerVectorFloat(_InnerVector, _abc.ABC):
    __slots__ = ()

    def _cypher_values_repr(self) -> str:
        res = str(self.to_native())
        return res.replace("nan", "NaN").replace("inf", "Infinity")


class _VecF64(_InnerVectorFloat):
    __slots__ = ()

    dtype = VectorDType.F64
    size = 8
    cypher_inner_type_repr = "FLOAT NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        if not all(isinstance(item, float) for item in data):
            for item in data:
                if not isinstance(item, float):
                    raise TypeError(
                        f"Cannot build f64 vector from {type(item).__name__}, "
                        "expected float."
                    )
        return cls(_struct.pack(f">{len(data)}d", *data))

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}d"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">f8"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.float64(), len(self), [None, buffer], 0
        )


class _VecF32(_InnerVectorFloat):
    __slots__ = ()

    dtype = VectorDType.F32
    size = 4
    cypher_inner_type_repr = "FLOAT32 NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        if not all(isinstance(item, float) for item in data):
            for item in data:
                if not isinstance(item, float):
                    raise TypeError(
                        f"Cannot build f32 vector from {type(item).__name__}, "
                        "expected float."
                    )
        try:
            bytes_ = _struct.pack(f">{len(data)}f", *data)
        except OverflowError:
            for item in data:
                try:
                    _struct.pack(">f", item)
                except OverflowError:
                    raise OverflowError(
                        f"Value {item} is out of range for f32: "
                        f"[-3.4028234e+38, 3.4028234e+38]"
                    ) from None
            raise
        return cls(bytes_)

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}f"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">f4"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.float32(), len(self), [None, buffer], 0
        )


class _InnerVectorInt(_InnerVector, _abc.ABC):
    __slots__ = ()

    def _cypher_values_repr(self) -> str:
        return str(self.to_native())


_I64_MIN = -9_223_372_036_854_775_808
_I64_MAX = 9_223_372_036_854_775_807


class _VecI64(_InnerVectorInt):
    __slots__ = ()

    dtype = VectorDType.I64
    size = 8
    cypher_inner_type_repr = "INTEGER NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        try:
            bytes_ = _struct.pack(f">{len(data)}q", *data)
        except _struct.error:
            for item in data:
                if not isinstance(item, int):
                    raise TypeError(
                        f"Cannot build i64 vector from {type(item).__name__}, "
                        "expected int."
                    ) from None
                if not _I64_MIN <= item <= _I64_MAX:
                    raise OverflowError(
                        f"Value {item} is out of range for i64: "
                        f"[{_I64_MIN}, {_I64_MAX}]"
                    ) from None
            raise
        return cls(bytes_)

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}q"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">i8"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.int64(), len(self), [None, buffer], 0
        )


_I32_MIN = -2_147_483_648
_I32_MAX = 2_147_483_647


class _VecI32(_InnerVectorInt):
    __slots__ = ()

    dtype = VectorDType.I32
    size = 4
    cypher_inner_type_repr = "INTEGER32 NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        try:
            bytes_ = _struct.pack(f">{len(data)}i", *data)
        except _struct.error:
            for item in data:
                if not isinstance(item, int):
                    raise TypeError(
                        f"Cannot build i32 vector from {type(item).__name__}, "
                        "expected int."
                    ) from None
                if not _I32_MIN <= item <= _I32_MAX:
                    raise OverflowError(
                        f"Value {item} is out of range for i32: "
                        f"[{_I32_MIN}, {_I32_MAX}]"
                    ) from None
            raise
        return cls(bytes_)

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}i"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">i4"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.int32(), len(self), [None, buffer], 0
        )


_I16_MIN = -32_768
_I16_MAX = 32_767


class _VecI16(_InnerVectorInt):
    __slots__ = ()

    dtype = VectorDType.I16
    size = 2
    cypher_inner_type_repr = "INTEGER16 NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        try:
            bytes_ = _struct.pack(f">{len(data)}h", *data)
        except _struct.error:
            for item in data:
                if not isinstance(item, int):
                    raise TypeError(
                        f"Cannot build i16 vector from {type(item).__name__}, "
                        "expected int."
                    ) from None
                if not _I16_MIN <= item <= _I16_MAX:
                    raise OverflowError(
                        f"Value {item} is out of range for i16: "
                        f"[{_I16_MIN}, {_I16_MAX}]"
                    ) from None
            raise
        return cls(bytes_)

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}h"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">i2"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.int16(), len(self), [None, buffer], 0
        )


_I8_MIN = -128
_I8_MAX = 127


class _VecI8(_InnerVectorInt):
    __slots__ = ()

    dtype = VectorDType.I8
    size = 1
    cypher_inner_type_repr = "INTEGER8 NOT NULL"

    @classmethod
    def from_native(cls, data: _t.Iterable[object], /) -> _t.Self:
        if not isinstance(data, _t.Sized):
            data = tuple(data)
        try:
            bytes_ = _struct.pack(f">{len(data)}b", *data)
        except _struct.error:
            for item in data:
                if not isinstance(item, int):
                    raise TypeError(
                        f"Cannot build i8 vector from {type(item).__name__}, "
                        "expected int."
                    ) from None
                if not _I8_MIN <= item <= _I8_MAX:
                    raise OverflowError(
                        f"Value {item} is out of range for i8: "
                        f"[{_I8_MIN}, {_I8_MAX}]"
                    ) from None
            raise
        return cls(bytes_)

    def to_native(self) -> list[object]:
        struct_format = f">{len(self.data) // self.size}b"
        return list(_struct.unpack(struct_format, self.data))

    def to_numpy(self) -> _np.ndarray:
        import numpy

        return numpy.frombuffer(self.data, dtype=numpy.dtype(">i1"))

    def to_pyarrow(self) -> _pa.Array:
        import pyarrow

        buffer = pyarrow.py_buffer(self.data_le)
        return pyarrow.Array.from_buffers(
            pyarrow.int8(), len(self), [None, buffer], 0
        )
