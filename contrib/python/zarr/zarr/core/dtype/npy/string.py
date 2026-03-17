from __future__ import annotations

import re
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Literal,
    Protocol,
    Self,
    TypedDict,
    TypeGuard,
    overload,
    runtime_checkable,
)

import numpy as np

from zarr.core.common import NamedConfig
from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeConfig_V2,
    DTypeJSON,
    HasEndianness,
    HasItemSize,
    HasLength,
    HasObjectCodec,
    check_dtype_spec_v2,
    v3_unstable_dtype_warning,
)
from zarr.core.dtype.npy.common import (
    check_json_str,
    endianness_to_numpy_str,
    get_endianness_from_numpy_dtype,
)
from zarr.core.dtype.wrapper import TDType_co, ZDType

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat
    from zarr.core.dtype.wrapper import TBaseDType

_NUMPY_SUPPORTS_VLEN_STRING = hasattr(np.dtypes, "StringDType")


@runtime_checkable
class SupportsStr(Protocol):
    def __str__(self) -> str: ...


class LengthBytesConfig(TypedDict):
    """
    Configuration for a fixed-length string data type in Zarr V3.

    Attributes
    ----------
    length_bytes : int
        The length in bytes of the data associated with this configuration.
    """

    length_bytes: int


class FixedLengthUTF32JSON_V2(DTypeConfig_V2[str, None]):
    """
    A wrapper around the JSON representation of the ``FixedLengthUTF32`` data type in Zarr V2.

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
        "name": "<U12",
        "object_codec_id": None
    }
    ```
    """


class FixedLengthUTF32JSON_V3(NamedConfig[Literal["fixed_length_utf32"], LengthBytesConfig]):
    """
    The JSON representation of the ``FixedLengthUTF32`` data type in Zarr V3.

    References
    ----------
    This representation is not currently defined in an external specification.

    Examples
    --------
    ```python
    {
        "name": "fixed_length_utf32",
        "configuration": {
            "length_bytes": 12
            }
    }
    ```
    """


@dataclass(frozen=True, kw_only=True)
class FixedLengthUTF32(
    ZDType[np.dtypes.StrDType[int], np.str_], HasEndianness, HasLength, HasItemSize
):
    """
    A Zarr data type for arrays containing fixed-length UTF-32 strings.

    Wraps the ``np.dtypes.StrDType`` data type. Scalars for this data type are instances of
    ``np.str_``.

    Attributes
    ----------
    dtype_cls : Type[np.dtypes.StrDType]
        The NumPy dtype class for this data type.
    _zarr_v3_name : ClassVar[Literal["fixed_length_utf32"]]
        The name of this data type in Zarr V3.
    code_point_bytes : ClassVar[int] = 4
        The number of bytes per code point in UTF-32, which is 4.
    """

    dtype_cls = np.dtypes.StrDType
    _zarr_v3_name: ClassVar[Literal["fixed_length_utf32"]] = "fixed_length_utf32"
    code_point_bytes: ClassVar[int] = 4  # utf32 is 4 bytes per code point

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
        Create a FixedLengthUTF32 from a NumPy data type.

        Parameters
        ----------
        dtype : TBaseDType
            The NumPy data type.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if cls._check_native_dtype(dtype):
            endianness = get_endianness_from_numpy_dtype(dtype)
            return cls(
                length=dtype.itemsize // (cls.code_point_bytes),
                endianness=endianness,
            )
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> np.dtypes.StrDType[int]:
        """
        Convert the FixedLengthUTF32 instance to a NumPy data type.

        Returns
        -------
        np.dtypes.StrDType[int]
            The NumPy data type.
        """
        byte_order = endianness_to_numpy_str(self.endianness)
        return self.dtype_cls(self.length).newbyteorder(byte_order)

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[FixedLengthUTF32JSON_V2]:
        """
        Check that the input is a valid JSON representation of a NumPy U dtype.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        TypeGuard[FixedLengthUTF32JSON_V2]
            Whether the input is a valid JSON representation of a NumPy U dtype.
        """
        return (
            check_dtype_spec_v2(data)
            and isinstance(data["name"], str)
            and re.match(r"^[><]U\d+$", data["name"]) is not None
            and data["object_codec_id"] is None
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[FixedLengthUTF32JSON_V3]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        TypeGuard[FixedLengthUTF32JSONV3]
            Whether the input is a valid JSON representation of a NumPy U dtype.
        """
        return (
            isinstance(data, dict)
            and set(data.keys()) == {"name", "configuration"}
            and data["name"] == cls._zarr_v3_name
            and "configuration" in data
            and isinstance(data["configuration"], dict)
            and set(data["configuration"].keys()) == {"length_bytes"}
            and isinstance(data["configuration"]["length_bytes"], int)
        )

    @overload
    def to_json(self, zarr_format: Literal[2]) -> DTypeConfig_V2[str, None]: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> FixedLengthUTF32JSON_V3: ...

    def to_json(
        self, zarr_format: ZarrFormat
    ) -> DTypeConfig_V2[str, None] | FixedLengthUTF32JSON_V3:
        """
        Convert the FixedLengthUTF32 instance to a JSON representation.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The Zarr format to use.

        Returns
        -------
        DTypeConfig_V2[str, None] | FixedLengthUTF32JSON_V3
            The JSON representation of the data type.
        """
        if zarr_format == 2:
            return {"name": self.to_native_dtype().str, "object_codec_id": None}
        elif zarr_format == 3:
            v3_unstable_dtype_warning(self)
            return {
                "name": self._zarr_v3_name,
                "configuration": {"length_bytes": self.length * self.code_point_bytes},
            }
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create a FixedLengthUTF32 from a JSON representation of a NumPy U dtype.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if cls._check_json_v2(data):
            # Construct the NumPy dtype instead of string parsing.
            name = data["name"]
            return cls.from_native_dtype(np.dtype(name))
        raise DataTypeValidationError(
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a string representation of a NumPy U dtype."
        )

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create a FixedLengthUTF32 from a JSON representation of a NumPy U dtype.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if cls._check_json_v3(data):
            return cls(length=data["configuration"]["length_bytes"] // cls.code_point_bytes)
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected {cls._zarr_v3_name}."
        raise DataTypeValidationError(msg)

    def default_scalar(self) -> np.str_:
        """
        Return the default scalar value for this data type.

        Returns
        -------
        ``np.str_``
            The default scalar value.
        """
        return np.str_("")

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> str:
        """
        Convert the scalar value to a JSON representation.

        Parameters
        ----------
        data : object
            The scalar value.
        zarr_format : ZarrFormat
            The Zarr format to use.

        Returns
        -------
        str
            The JSON representation of the scalar value.
        """
        return str(data)

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.str_:
        """
        Convert the JSON representation of a scalar value to the native scalar value.

        Parameters
        ----------
        data : JSON
            The JSON data.
        zarr_format : ZarrFormat
            The Zarr format to use.

        Returns
        -------
        ``np.str_``
            The native scalar value.
        """
        if check_json_str(data):
            return self.to_native_dtype().type(data)
        raise TypeError(f"Invalid type: {data}. Expected a string.")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[SupportsStr]:
        """
        Check that the input is a valid scalar value for this data type.

        Parameters
        ----------
        data : object
            The scalar value.

        Returns
        -------
        TypeGuard[SupportsStr]
            Whether the input is a valid scalar value for this data type.
        """
        # this is generous for backwards compatibility
        return isinstance(data, SupportsStr)

    def cast_scalar(self, data: object) -> np.str_:
        """
        Cast the scalar value to the native scalar value.

        Parameters
        ----------
        data : object
            The scalar value.

        Returns
        -------
        ``np.str_``
            The native scalar value.
        """
        if self._check_scalar(data):
            # We explicitly truncate before casting because of the following NumPy behavior:
            # >>> x = np.dtype('U3').type('hello world')
            # >>> x
            # np.str_('hello world')
            # >>> x.dtype
            # dtype('U11')

            return self.to_native_dtype().type(str(data)[: self.length])

        msg = (  # pragma: no cover
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)  # pragma: no-cover

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return self.length * self.code_point_bytes


def check_vlen_string_json_scalar(data: object) -> TypeGuard[int | str | float]:
    """
    Check if the input is a valid JSON scalar for a variable-length string.

    This function is generous for backwards compatibility, as Zarr Python v2 would use ints for
    variable-length string fill values.

    Parameters
    ----------
    data : object
        The JSON value to check.

    Returns
    -------
    TypeGuard[int | str | float]
        True if the input is a valid scalar for a variable-length string.
    """
    return isinstance(data, int | str | float)


class VariableLengthUTF8JSON_V2(DTypeConfig_V2[Literal["|O"], Literal["vlen-utf8"]]):
    """
    A wrapper around the JSON representation of the ``VariableLengthUTF8`` data type in Zarr V2.

    The ``name`` field of this class contains the value that would appear under the
    ``dtype`` field in Zarr V2 array metadata. The ``object_codec_id`` field is always ``"vlen-utf8"``.

    References
    ----------
    The structure of the ``name`` field is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).


    Examples
    --------
    ```python
    {
        "name": "|O",
        "object_codec_id": "vlen-utf8"
    }
    ```
    """


# VariableLengthUTF8 is defined in two places, conditioned on the version of NumPy.
# If NumPy 2 is installed, then VariableLengthUTF8 is defined with the NumPy variable length
# string dtype as the native dtype. Otherwise, VariableLengthUTF8 is defined with the NumPy object
# dtype as the native dtype.
class UTF8Base(ZDType[TDType_co, str], HasObjectCodec):
    """
    A base class for variable-length UTF-8 string data types.

    Not intended for direct use, but as a base for concrete implementations.

    Attributes
    ----------
    object_codec_id : ClassVar[Literal["vlen-utf8"]]
        The object codec ID for this data type.

    References
    ----------
    This data type does not have a Zarr V3 specification.

    The Zarr V2 data type specification can be found [here](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).
    """

    _zarr_v3_name: ClassVar[Literal["string"]] = "string"
    object_codec_id: ClassVar[Literal["vlen-utf8"]] = "vlen-utf8"

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of this data type from a compatible NumPy data type.


        Parameters
        ----------
        dtype : TBaseDType
            The native data type.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input is not compatible with this data type.
        """
        if cls._check_native_dtype(dtype):
            return cls()
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    @classmethod
    def _check_json_v2(
        cls,
        data: DTypeJSON,
    ) -> TypeGuard[VariableLengthUTF8JSON_V2]:
        """
        "Check if the input is a valid JSON representation of a variable-length UTF-8 string dtype
        for Zarr v2."

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        ``TypeGuard[VariableLengthUTF8JSON_V2]``
            Whether the input is a valid JSON representation of a NumPy "object" data type, and that the
            object codec id is appropriate for variable-length UTF-8 strings.
        """
        return (
            check_dtype_spec_v2(data)
            and data["name"] == "|O"
            and data["object_codec_id"] == cls.object_codec_id
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[Literal["variable_length_utf8"]]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[Literal["variable_length_utf8"]]
            Whether the input is a valid JSON representation of a variable length UTF-8 string
            data type.
        """
        return data == cls._zarr_v3_name

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from a JSON representation of a NumPy "object" dtype.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to create an instance from.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if cls._check_json_v2(data):
            return cls()
        msg = (
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected the string '|O'"
        )
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from a JSON representation of a variable length UTF-8
        string data type.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to create an instance from.

        Returns
        -------
        Self
            An instance of this data type.
        """
        if cls._check_json_v3(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected {cls._zarr_v3_name}."
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> VariableLengthUTF8JSON_V2: ...
    @overload
    def to_json(self, zarr_format: Literal[3]) -> Literal["string"]: ...

    def to_json(self, zarr_format: ZarrFormat) -> VariableLengthUTF8JSON_V2 | Literal["string"]:
        """
        Convert this data type to a JSON representation.

        Parameters
        ----------
        zarr_format : int
            The zarr format to use for the JSON representation.

        Returns
        -------
        ``VariableLengthUTF8JSON_V2 | Literal["string"]``
            The JSON representation of this data type.
        """
        if zarr_format == 2:
            return {"name": "|O", "object_codec_id": self.object_codec_id}
        elif zarr_format == 3:
            return self._zarr_v3_name
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def default_scalar(self) -> str:
        """
        Return the default scalar value for this data type.

        Returns
        -------
        str
            The default scalar value.
        """
        return ""

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> str:
        """
        Convert a scalar value to a JSON representation.

        Parameters
        ----------
        data : object
            The scalar value to convert.
        zarr_format : int
            The zarr format to use for the JSON representation.

        Returns
        -------
        str
            The JSON representation of the scalar value.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        raise TypeError(f"Invalid type: {data}. Expected a string.")

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> str:
        """
        Convert a JSON representation of a scalar value to the native scalar type.

        Parameters
        ----------
        data : JSON
            The JSON representation of the scalar value.
        zarr_format : int
            The zarr format to use for the JSON representation.

        Returns
        -------
        str
            The native scalar type of the scalar value.
        """
        if not check_vlen_string_json_scalar(data):
            raise TypeError(f"Invalid type: {data}. Expected a string or number.")
        return str(data)

    def _check_scalar(self, data: object) -> TypeGuard[SupportsStr]:
        """
        Check that the input is a valid scalar value for this data type.

        Parameters
        ----------
        data : object
            The scalar value to check.

        Returns
        -------
        TypeGuard[SupportsStr]
            Whether the input is a valid scalar value for this data type.
        """
        return isinstance(data, SupportsStr)

    def _cast_scalar_unchecked(self, data: SupportsStr) -> str:
        """
        Cast a scalar value to a string.

        Parameters
        ----------
        data : object
            The scalar value to cast.

        Returns
        -------
        str
            The string representation of the scalar value.
        """
        return str(data)

    def cast_scalar(self, data: object) -> str:
        """
        Cast an object to a string.

        Parameters
        ----------
        data : object
            The value to cast.

        Returns
        -------
        str
            The input cast to str.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (  # pragma: no cover
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)  # pragma: no cover


if _NUMPY_SUPPORTS_VLEN_STRING:

    @dataclass(frozen=True, kw_only=True)
    class VariableLengthUTF8(UTF8Base[np.dtypes.StringDType]):  # type: ignore[type-var]
        """
        A Zarr data type for arrays containing variable-length UTF-8 strings.

        Wraps the ``np.dtypes.StringDType`` data type. Scalars for this data type are instances
        of ``str``.


        Attributes
        ----------
        dtype_cls : Type[np.dtypes.StringDType]
            The NumPy dtype class for this data type.
        _zarr_v3_name : ClassVar[Literal["variable_length_utf8"]] = "variable_length_utf8"
            The name of this data type in Zarr V3.
        object_codec_id : ClassVar[Literal["vlen-utf8"]] = "vlen-utf8"
            The object codec ID for this data type.
        """

        dtype_cls = np.dtypes.StringDType

        def to_native_dtype(self) -> np.dtypes.StringDType:
            """
            Create a NumPy string dtype from this VariableLengthUTF8 ZDType.

            Returns
            -------
            np.dtypes.StringDType
                The NumPy string dtype.
            """
            return self.dtype_cls()

else:
    # Numpy pre-2 does not have a variable length string dtype, so we use the Object dtype instead.
    @dataclass(frozen=True, kw_only=True)
    class VariableLengthUTF8(UTF8Base[np.dtypes.ObjectDType]):  # type: ignore[no-redef]
        """
        A Zarr data type for arrays containing variable-length UTF-8 strings.

        Wraps the ``np.dtypes.ObjectDType`` data type. Scalars for this data type are instances
        of ``str``.


        Attributes
        ----------
        dtype_cls : Type[np.dtypes.ObjectDType]
            The NumPy dtype class for this data type.
        _zarr_v3_name : ClassVar[Literal["variable_length_utf8"]] = "variable_length_utf8"
            The name of this data type in Zarr V3.
        object_codec_id : ClassVar[Literal["vlen-utf8"]] = "vlen-utf8"
            The object codec ID for this data type.
        """

        dtype_cls = np.dtypes.ObjectDType

        def to_native_dtype(self) -> np.dtypes.ObjectDType:
            """
            Create a NumPy object dtype from this VariableLengthUTF8 ZDType.

            Returns
            -------
            np.dtypes.ObjectDType
                The NumPy object dtype.
            """
            return self.dtype_cls()
