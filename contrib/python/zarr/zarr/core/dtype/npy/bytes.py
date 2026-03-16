from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from typing import ClassVar, Literal, Self, TypedDict, TypeGuard, cast, overload

import numpy as np

from zarr.core.common import JSON, NamedConfig, ZarrFormat
from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeConfig_V2,
    DTypeJSON,
    HasItemSize,
    HasLength,
    HasObjectCodec,
    check_dtype_spec_v2,
    v3_unstable_dtype_warning,
)
from zarr.core.dtype.npy.common import check_json_str
from zarr.core.dtype.wrapper import TBaseDType, ZDType

BytesLike = np.bytes_ | str | bytes | int


class FixedLengthBytesConfig(TypedDict):
    """
    A configuration for a data type that takes a ``length_bytes`` parameter.

    Attributes
    ----------

    length_bytes : int
        The length in bytes of the data associated with this configuration.

    Examples
    --------
    ```python
    {
        "length_bytes": 12
    }
    ```
    """

    length_bytes: int


class NullterminatedBytesJSON_V2(DTypeConfig_V2[str, None]):
    """
    A wrapper around the JSON representation of the ``NullTerminatedBytes`` data type in Zarr V2.

    The ``name`` field of this class contains the value that would appear under the
    ``dtype`` field in Zarr V2 array metadata.

    References
    ----------
    The structure of the ``name`` field is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).


    Examples
    --------
    ```python
    {
        "name": "|S10",
        "object_codec_id": None
    }
    ```
    """


class NullTerminatedBytesJSON_V3(
    NamedConfig[Literal["null_terminated_bytes"], FixedLengthBytesConfig]
):
    """
    The JSON representation of the ``NullTerminatedBytes`` data type in Zarr V3.

    References
    ----------
    This representation is not currently defined in an external specification.


    Examples
    --------
    ```python
    {
        "name": "null_terminated_bytes",
        "configuration": {
            "length_bytes": 12
        }
    }
    ```

    """


class RawBytesJSON_V2(DTypeConfig_V2[str, None]):
    """
    A wrapper around the JSON representation of the ``RawBytes`` data type in Zarr V2.

    The ``name`` field of this class contains the value that would appear under the
    ``dtype`` field in Zarr V2 array metadata.

    References
    ----------
    The structure of the ``name`` field is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).


    Examples
    --------
    ```python

        {
            "name": "|V10",
            "object_codec_id": None
        }
    ```
    """


class RawBytesJSON_V3(NamedConfig[Literal["raw_bytes"], FixedLengthBytesConfig]):
    """
    The JSON representation of the ``RawBytes`` data type in Zarr V3.

    References
    ----------
    This representation is not currently defined in an external specification.


    Examples
    --------
    ```python
    {
        "name": "raw_bytes",
        "configuration": {
            "length_bytes": 12
        }
    }
    ```
    """


class VariableLengthBytesJSON_V2(DTypeConfig_V2[Literal["|O"], Literal["vlen-bytes"]]):
    """
    A wrapper around the JSON representation of the ``VariableLengthBytes`` data type in Zarr V2.

    The ``name`` field of this class contains the value that would appear under the
    ``dtype`` field in Zarr V2 array metadata. The ``object_codec_id`` field is always ``"vlen-bytes"``

    References
    ----------
    The structure of the ``name`` field is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).

    Examples
    --------
    ```python
    {
        "name": "|O",
        "object_codec_id": "vlen-bytes"
    }
    ```
    """


@dataclass(frozen=True, kw_only=True)
class NullTerminatedBytes(ZDType[np.dtypes.BytesDType[int], np.bytes_], HasLength, HasItemSize):
    """
    A Zarr data type for arrays containing fixed-length null-terminated byte sequences.

    Wraps the [`np.dtypes.BytesDType`][numpy.dtypes.BytesDType] data type. Scalars for this data type are instances of
    [`np.bytes_`][numpy.bytes_].

    This data type is parametrized by an integral length which specifies size in bytes of each
    scalar. Because this data type uses null-terminated semantics, indexing into
    NumPy arrays with this data type may return fewer than ``length`` bytes.

    Attributes
    ----------
    dtype_cls: ClassVar[type[np.dtypes.BytesDType[int]]] = np.dtypes.BytesDType
        The NumPy data type wrapped by this ZDType.
    _zarr_v3_name : ClassVar[Literal["null_terminated_bytes"]]
    length : int
        The length of the bytes.

    Notes
    -----
    This data type is designed for compatibility with NumPy arrays that use the NumPy ``bytes`` data type.
    It may not be desirable for usage outside of that context. If compatibility
    with the NumPy ``bytes`` data type is not essential, consider using the ``RawBytes``
    or ``VariableLengthBytes`` data types instead.
    """

    dtype_cls = np.dtypes.BytesDType
    _zarr_v3_name: ClassVar[Literal["null_terminated_bytes"]] = "null_terminated_bytes"

    def __post_init__(self) -> None:
        """
        We don't allow instances of this class with length less than 1 because there is no way such
        a data type can contain actual data.
        """
        if self.length < 1:
            raise ValueError(f"length must be >= 1, got {self.length}.")

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of NullTerminatedBytes from an instance of np.dtypes.BytesDType.

        This method checks if the provided data type is an instance of np.dtypes.BytesDType.
        If so, it returns a new instance of NullTerminatedBytes with a length equal to the
        length of input data type.

        Parameters
        ----------
        dtype : TBaseDType
            The native dtype to convert.

        Returns
        -------
        NullTerminatedBytes
            An instance of NullTerminatedBytes with the specified length.

        Raises
        ------
        DataTypeValidationError
            If the dtype is not compatible with NullTerminatedBytes.
        """

        if cls._check_native_dtype(dtype):
            return cls(length=dtype.itemsize)
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> np.dtypes.BytesDType[int]:
        """
        Create a NumPy bytes dtype from this NullTerminatedBytes ZDType.

        Returns
        -------
        np.dtypes.BytesDType[int]
            A NumPy data type object representing null-terminated bytes with a specified length.
        """

        return self.dtype_cls(self.length)

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[NullterminatedBytesJSON_V2]:
        """
        Check that the input is a valid JSON representation of NullTerminatedBytes in Zarr V2.

        The input data must be a mapping that contains a "name" key that matches the pattern
        "|S<number>" and an "object_codec_id" key that is None.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        bool
            True if the input data is a valid representation, False otherwise.
        """

        return (
            check_dtype_spec_v2(data)
            and isinstance(data["name"], str)
            and re.match(r"^\|S\d+$", data["name"]) is not None
            and data["object_codec_id"] is None
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[NullTerminatedBytesJSON_V3]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[NullTerminatedBytesJSON_V3]
            True if the input is a valid representation of this class in Zarr V3, False
            otherwise.
        """
        return (
            isinstance(data, dict)
            and set(data.keys()) == {"name", "configuration"}
            and data["name"] == cls._zarr_v3_name
            and isinstance(data["configuration"], dict)
            and "length_bytes" in data["configuration"]
            and isinstance(data["configuration"]["length_bytes"], int)
        )

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from Zarr V2-flavored JSON.

        This method checks if the input data is a valid representation of
        this class in Zarr V2. If so, it returns a new instance of
        this class with a ``length`` as specified in the input data.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class.
        """

        if cls._check_json_v2(data):
            name = data["name"]
            return cls(length=int(name[2:]))
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a string like '|S1', '|S2', etc"
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from Zarr V3-flavored JSON.

        This method checks if the input data is a valid representation of
        this class in Zarr V3. If so, it returns a new instance of
        this class with a ``length`` as specified in the input data.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class.
        """
        if cls._check_json_v3(data):
            return cls(length=data["configuration"]["length_bytes"])
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string {cls._zarr_v3_name!r}"
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> NullterminatedBytesJSON_V2: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> NullTerminatedBytesJSON_V3: ...

    def to_json(
        self, zarr_format: ZarrFormat
    ) -> DTypeConfig_V2[str, None] | NullTerminatedBytesJSON_V3:
        """
        Generate a JSON representation of this data type.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        NullterminatedBytesJSON_V2 | NullTerminatedBytesJSON_V3
            The JSON-serializable representation of the data type
        """
        if zarr_format == 2:
            return {"name": self.to_native_dtype().str, "object_codec_id": None}
        elif zarr_format == 3:
            v3_unstable_dtype_warning(self)
            return {
                "name": self._zarr_v3_name,
                "configuration": {"length_bytes": self.length},
            }
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[BytesLike]:
        """
        Check if the provided data is of type BytesLike.

        This method is used to verify if the input data can be considered as a
        scalar of bytes-like type, which includes NumPy bytes, strings, bytes,
        and integers.

        Parameters
        ----------
        data : object
            The data to check.

        Returns
        -------
        TypeGuard[BytesLike]
            True if the data is bytes-like, False otherwise.
        """

        return isinstance(data, BytesLike)

    def _cast_scalar_unchecked(self, data: BytesLike) -> np.bytes_:
        """
        Cast the provided scalar data to [`np.bytes_`][numpy.bytes_], truncating if necessary.

        Parameters
        ----------
        data : BytesLike
            The data to cast.

        Returns
        -------
        bytes : [`np.bytes_`][numpy.bytes_]
            The casted data as a NumPy bytes scalar.

        Notes
        -----
        This method does not perform any type checking.
        The input data must be bytes-like.
        """
        if isinstance(data, int):
            return self.to_native_dtype().type(str(data)[: self.length])
        else:
            return self.to_native_dtype().type(data[: self.length])

    def cast_scalar(self, data: object) -> np.bytes_:
        """
        Attempt to cast a given object to a NumPy bytes scalar.

        This method first checks if the provided data is a valid scalar that can be
        converted to a NumPy bytes scalar. If the check succeeds, the unchecked casting
        operation is performed. If the data is not valid, a TypeError is raised.

        Parameters
        ----------
        data : object
            The data to be cast to a NumPy bytes scalar.

        Returns
        -------
        bytes : [`np.bytes_`][numpy.bytes_]
            The data cast as a NumPy bytes scalar.

        Raises
        ------
        TypeError
            If the data cannot be converted to a NumPy bytes scalar.
        """

        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)

    def default_scalar(self) -> np.bytes_:
        """
        Return a default scalar value, which for this data type is an empty byte string.

        Returns
        -------
        bytes : [`np.bytes_`][numpy.bytes_]
            The default scalar value.
        """
        return np.bytes_(b"")

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> str:
        """
        Convert a scalar to a JSON-serializable string representation.

        This method encodes the given scalar as a NumPy bytes scalar and then
        encodes the bytes as a base64-encoded string.

        Parameters
        ----------
        data : object
            The scalar to convert.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        str
            A string representation of the scalar.
        """
        as_bytes = self.cast_scalar(data)
        return base64.standard_b64encode(as_bytes).decode("ascii")

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.bytes_:
        """
        Read a JSON-serializable value as [`np.bytes_`][numpy.bytes_].

        Parameters
        ----------
        data : JSON
            The JSON-serializable base64-encoded string.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        bytes : [`np.bytes_`][numpy.bytes_]
            The NumPy bytes scalar obtained from decoding the base64 string.

        Raises
        ------
        TypeError
            If the input data is not a base64-encoded string.
        """

        if check_json_str(data):
            return self.to_native_dtype().type(base64.standard_b64decode(data.encode("ascii")))
        raise TypeError(
            f"Invalid type: {data}. Expected a base64-encoded string."
        )  # pragma: no cover

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return self.length


@dataclass(frozen=True, kw_only=True)
class RawBytes(ZDType[np.dtypes.VoidDType[int], np.void], HasLength, HasItemSize):
    """
    A Zarr data type for arrays containing fixed-length sequences of raw bytes.

    Wraps the NumPy ``void`` data type. Scalars for this data type are instances of [`np.void`][numpy.void].

    This data type is parametrized by an integral length which specifies size in bytes of each
    scalar belonging to this data type.

    Attributes
    ----------
    dtype_cls: ClassVar[type[np.dtypes.VoidDType[int]]] = np.dtypes.VoidDtype
        The NumPy data type wrapped by this ZDType.
    _zarr_v3_name : ClassVar[Literal["raw_bytes"]]
    length : int
        The length of the bytes.

    Notes
    -----
    Although the NumPy "Void" data type is used to create "structured" data types in NumPy, this
    class does not support structured data types.

    See the ``Structured`` data type for this functionality.

    """

    # np.dtypes.VoidDType is specified in an odd way in NumPy
    # it cannot be used to create instances of the dtype
    # so we have to tell mypy to ignore this here
    dtype_cls = np.dtypes.VoidDType  # type: ignore[assignment]
    _zarr_v3_name: ClassVar[Literal["raw_bytes"]] = "raw_bytes"

    def __post_init__(self) -> None:
        """
        We don't allow instances of this class with length less than 1 because there is no way such
        a data type can contain actual data.
        """
        if self.length < 1:
            raise ValueError(f"length must be >= 1, got {self.length}.")

    @classmethod
    def _check_native_dtype(
        cls: type[Self], dtype: TBaseDType
    ) -> TypeGuard[np.dtypes.VoidDType[int]]:
        """
        Check that the input is a NumPy void dtype with no fields.


        Numpy void dtype comes in two forms:
        * If the ``fields`` attribute is ``None``, then the dtype represents N raw bytes.
        * If the ``fields`` attribute is not ``None``, then the dtype represents a structured dtype,

        In this check we ensure that ``fields`` is ``None``.

        Parameters
        ----------
        dtype : TDBaseDType
            The dtype to check.

        Returns
        -------
        Bool
            True if the dtype is an instance of np.dtypes.VoidDType with no fields, False otherwise.
        """
        return cls.dtype_cls is type(dtype) and dtype.fields is None

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of RawBytes from an instance of np.dtypes.VoidDType.

        This method checks if the provided data type is compatible with RawBytes. The input
        must be an instance of np.dtypes.VoidDType, and have no fields. If the input is compatible,
        this method returns an instance of RawBytes with the specified length.


        Parameters
        ----------
        dtype : TBaseDType
            The native dtype to convert.

        Returns
        -------
        RawBytes
            An instance of RawBytes with the specified length.

        Raises
        ------
        DataTypeValidationError
            If the dtype is not compatible with RawBytes.
        """

        if cls._check_native_dtype(dtype):
            return cls(length=dtype.itemsize)
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> np.dtypes.VoidDType[int]:
        """
        Create a NumPy void dtype from this RawBytes ZDType.

        Returns
        -------
        np.dtypes.VoidDType[int]
            A NumPy data type object representing raw bytes with a specified length.
        """
        # Numpy does not allow creating a void type
        # by invoking np.dtypes.VoidDType directly
        return cast("np.dtypes.VoidDType[int]", np.dtype(f"V{self.length}"))

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[RawBytesJSON_V2]:
        """
        Check that the input is a valid representation of this class in Zarr V2.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        True if the input is a valid representation of this class in Zarr V3, False otherwise.

        """
        return (
            check_dtype_spec_v2(data)
            and isinstance(data["name"], str)
            and re.match(r"^\|V\d+$", data["name"]) is not None
            and data["object_codec_id"] is None
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[RawBytesJSON_V3]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[RawBytesJSON_V3]
            True if the input is a valid representation of this class in Zarr V3, False
            otherwise.
        """
        return (
            isinstance(data, dict)
            and set(data.keys()) == {"name", "configuration"}
            and data["name"] == cls._zarr_v3_name
            and isinstance(data["configuration"], dict)
            and set(data["configuration"].keys()) == {"length_bytes"}
            and isinstance(data["configuration"]["length_bytes"], int)
        )

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of RawBytes from Zarr V2-flavored JSON.

        This method checks if the input data is a valid representation of
        RawBytes in Zarr V2. If so, it returns a new instance of
        RawBytes with a ``length`` as specified in the input data.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class.
        """
        if cls._check_json_v2(data):
            name = data["name"]
            return cls(length=int(name[2:]))
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a string like '|V1', '|V2', etc"
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of RawBytes from Zarr V3-flavored JSON.

        This method checks if the input data is a valid representation of
        RawBytes in Zarr V3. If so, it returns a new instance of
        RawBytes with a ``length`` as specified in the input data.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        RawBytes
            An instance of RawBytes.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class.
        """
        if cls._check_json_v3(data):
            return cls(length=data["configuration"]["length_bytes"])
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string {cls._zarr_v3_name!r}"
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> RawBytesJSON_V2: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> RawBytesJSON_V3: ...

    def to_json(self, zarr_format: ZarrFormat) -> RawBytesJSON_V2 | RawBytesJSON_V3:
        """
        Generate a JSON representation of this data type.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        RawBytesJSON_V2 | RawBytesJSON_V3
            The JSON-serializable representation of the data type.
        """
        if zarr_format == 2:
            return {"name": self.to_native_dtype().str, "object_codec_id": None}
        elif zarr_format == 3:
            v3_unstable_dtype_warning(self)
            return {"name": self._zarr_v3_name, "configuration": {"length_bytes": self.length}}
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[np.bytes_ | str | bytes | np.void]:
        """
        Check if the provided data can be cast to np.void.

        This method is used to verify if the input data can be considered as a
        scalar of bytes-like type, which includes np.bytes_, np.void, strings, and bytes objects.

        Parameters
        ----------
        data : object
            The data to check.

        Returns
        -------
        TypeGuard[np.bytes_ | str | bytes | np.void]
            True if the data is void-scalar-like, False otherwise.
        """
        return isinstance(data, np.bytes_ | str | bytes | np.void)

    def _cast_scalar_unchecked(self, data: object) -> np.void:
        """
        Cast the provided scalar data to np.void.

        Parameters
        ----------
        data : BytesLike
            The data to cast.

        Returns
        -------
        np.void
            The casted data as a NumPy void scalar.

        Notes
        -----
        This method does not perform any type checking.
        The input data must be castable to np.void.
        """
        native_dtype = self.to_native_dtype()
        # Without the second argument, NumPy will return a void scalar for dtype V1.
        # The second argument ensures that, if native_dtype is something like V10,
        # the result will actually be a V10 scalar.
        return native_dtype.type(data, native_dtype)

    def cast_scalar(self, data: object) -> np.void:
        """
        Attempt to cast a given object to a NumPy void scalar.

        This method first checks if the provided data is a valid scalar that can be
        converted to a NumPy void scalar. If the check succeeds, the unchecked casting
        operation is performed. If the data is not valid, a TypeError is raised.

        Parameters
        ----------
        data : object
            The data to be cast to a NumPy void scalar.

        Returns
        -------
        np.void
            The data cast as a NumPy void scalar.

        Raises
        ------
        TypeError
            If the data cannot be converted to a NumPy void scalar.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)

    def default_scalar(self) -> np.void:
        """
        Return the default scalar value for this data type.

        The default scalar is a NumPy void scalar of the same length as the data type,
        filled with zero bytes.

        Returns
        -------
        np.void
            The default scalar value.
        """
        return self.to_native_dtype().type(("\x00" * self.length).encode("ascii"))

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> str:
        """
        Convert a scalar to a JSON-serializable string representation.

        This method converts the given scalar to bytes and then
        encodes the bytes as a base64-encoded string.

        Parameters
        ----------
        data : object
            The scalar to convert.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        str
            A string representation of the scalar.
        """
        as_bytes = self.cast_scalar(data)
        return base64.standard_b64encode(as_bytes.tobytes()).decode("ascii")

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.void:
        """
        Read a JSON-serializable value as a np.void.

        Parameters
        ----------
        data : JSON
            The JSON-serializable value.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        np.void
            The NumPy void scalar.

        Raises
        ------
        TypeError
            If the data is not a string, or if the string is not a valid base64 encoding.
        """
        if check_json_str(data):
            return self.to_native_dtype().type(base64.standard_b64decode(data))
        raise TypeError(f"Invalid type: {data}. Expected a string.")  # pragma: no cover

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return self.length


@dataclass(frozen=True, kw_only=True)
class VariableLengthBytes(ZDType[np.dtypes.ObjectDType, bytes], HasObjectCodec):
    """
    A Zarr data type for arrays containing variable-length sequences of bytes.

    Wraps the NumPy "object" data type. Scalars for this data type are instances of ``bytes``.

    Attributes
    ----------
    dtype_cls: ClassVar[type[np.dtypes.ObjectDType]] = np.dtypes.ObjectDType
        The NumPy data type wrapped by this ZDType.
    _zarr_v3_name: ClassVar[Literal["variable_length_bytes"]] = "variable_length_bytes"
        The name of this data type in Zarr V3.
    object_codec_id: ClassVar[Literal["vlen-bytes"]] = "vlen-bytes"
        The object codec ID for this data type.

    Notes
    -----
    Because this data type uses the NumPy "object" data type, it does not guarantee a compact memory
    representation of array data. Therefore a "vlen-bytes" codec is needed to ensure that the array
    data can be persisted to storage.
    """

    dtype_cls = np.dtypes.ObjectDType
    _zarr_v3_name: ClassVar[Literal["variable_length_bytes"]] = "variable_length_bytes"
    object_codec_id: ClassVar[Literal["vlen-bytes"]] = "vlen-bytes"

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of VariableLengthBytes from an instance of np.dtypes.ObjectDType.

        This method checks if the provided data type is an instance of np.dtypes.ObjectDType.
        If so, it returns an instance of VariableLengthBytes.

        Parameters
        ----------
        dtype : TBaseDType
            The native dtype to convert.

        Returns
        -------
        VariableLengthBytes
            An instance of VariableLengthBytes.

        Raises
        ------
        DataTypeValidationError
            If the dtype is not compatible with VariableLengthBytes.
        """
        if cls._check_native_dtype(dtype):
            return cls()
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> np.dtypes.ObjectDType:
        """
        Create a NumPy object dtype from this VariableLengthBytes ZDType.

        Returns
        -------
        np.dtypes.ObjectDType
            A NumPy data type object representing variable-length bytes.
        """
        return self.dtype_cls()

    @classmethod
    def _check_json_v2(
        cls,
        data: DTypeJSON,
    ) -> TypeGuard[VariableLengthBytesJSON_V2]:
        """
        Check that the input is a valid JSON representation of a NumPy O dtype, and that the
        object codec id is appropriate for variable-length bytes strings.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        True if the input is a valid representation of this class in Zarr V2, False
        otherwise.
        """
        # Check that the input is a valid JSON representation of a Zarr v2 data type spec.
        if not check_dtype_spec_v2(data):
            return False

        # Check that the object codec id is appropriate for variable-length bytes strings.
        if data["name"] != "|O":
            return False
        return data["object_codec_id"] == cls.object_codec_id

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[Literal["variable_length_bytes"]]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[Literal["variable_length_bytes"]]
            True if the input is a valid representation of this class in Zarr V3, False otherwise.
        """

        return data in (cls._zarr_v3_name, "bytes")

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this VariableLengthBytes from Zarr V2-flavored JSON.

        This method checks if the input data is a valid representation of this class
        in Zarr V2. If so, it returns a new instance this class.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class class.
        """

        if cls._check_json_v2(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string '|O' and an object_codec_id of {cls.object_codec_id}"
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of VariableLengthBytes from Zarr V3-flavored JSON.

        This method checks if the input data is a valid representation of
        VariableLengthBytes in Zarr V3. If so, it returns a new instance of
        VariableLengthBytes.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to parse.

        Returns
        -------
        VariableLengthBytes
            An instance of VariableLengthBytes.

        Raises
        ------
        DataTypeValidationError
            If the input data is not a valid representation of this class.
        """

        if cls._check_json_v3(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string {cls._zarr_v3_name!r}"
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> VariableLengthBytesJSON_V2: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> Literal["variable_length_bytes"]: ...

    def to_json(
        self, zarr_format: ZarrFormat
    ) -> VariableLengthBytesJSON_V2 | Literal["variable_length_bytes"]:
        """
        Convert the variable-length bytes data type to a JSON-serializable form.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The zarr format version. Accepted values are 2 and 3.

        Returns
        -------
        ``DTypeConfig_V2[Literal["|O"], Literal["vlen-bytes"]] | Literal["variable_length_bytes"]``
            The JSON-serializable representation of the variable-length bytes data type.
            For zarr_format 2, returns a dictionary with "name" and "object_codec_id".
            For zarr_format 3, returns a string identifier "variable_length_bytes".

        Raises
        ------
        ValueError
            If zarr_format is not 2 or 3.
        """

        if zarr_format == 2:
            return {"name": "|O", "object_codec_id": self.object_codec_id}
        elif zarr_format == 3:
            v3_unstable_dtype_warning(self)
            return self._zarr_v3_name
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def default_scalar(self) -> bytes:
        """
        Return the default scalar value for the variable-length bytes data type.

        Returns
        -------
        bytes
            The default scalar value, which is an empty byte string.
        """

        return b""

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> str:
        """
        Convert a scalar to a JSON-serializable string representation.

        This method encodes the given scalar as bytes and then
        encodes the bytes as a base64-encoded string.

        Parameters
        ----------
        data : object
            The scalar to convert.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        str
            A string representation of the scalar.
        """
        return base64.standard_b64encode(data).decode("ascii")  # type: ignore[arg-type]

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> bytes:
        """
        Decode a base64-encoded JSON string to bytes.

        Parameters
        ----------
        data : JSON
            The JSON-serializable base64-encoded string.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        bytes
            The decoded bytes from the base64 string.

        Raises
        ------
        TypeError
            If the input data is not a base64-encoded string.
        """

        if check_json_str(data):
            return base64.standard_b64decode(data.encode("ascii"))
        raise TypeError(f"Invalid type: {data}. Expected a string.")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[BytesLike]:
        """
        Check if the provided data is of type BytesLike.

        This method is used to verify if the input data can be considered as a
        scalar of bytes-like type, which includes NumPy bytes, strings, bytes,
        and integers.

        Parameters
        ----------
        data : object
            The data to check.

        Returns
        -------
        TypeGuard[BytesLike]
            True if the data is bytes-like, False otherwise.
        """
        return isinstance(data, BytesLike)

    def _cast_scalar_unchecked(self, data: BytesLike) -> bytes:
        """
        Cast the provided scalar data to bytes.

        Parameters
        ----------
        data : BytesLike
            The data to cast.

        Returns
        -------
        bytes
            The casted data as bytes.

        Notes
        -----
        This method does not perform any type checking.
        The input data must be bytes-like.
        """
        if isinstance(data, str):
            return bytes(data, encoding="utf-8")
        return bytes(data)

    def cast_scalar(self, data: object) -> bytes:
        """
        Attempt to cast a given object to a bytes scalar.

        This method first checks if the provided data is a valid scalar that can be
        converted to a bytes scalar. If the check succeeds, the unchecked casting
        operation is performed. If the data is not valid, a TypeError is raised.

        Parameters
        ----------
        data : object
            The data to be cast to a bytes scalar.

        Returns
        -------
        bytes
            The data cast as a bytes scalar.

        Raises
        ------
        TypeError
            If the data cannot be converted to a bytes scalar.
        """

        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)
