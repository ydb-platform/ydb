from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Literal,
    Self,
    TypeGuard,
    overload,
)

import numpy as np

from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeConfig_V2,
    DTypeJSON,
    HasEndianness,
    HasItemSize,
    check_dtype_spec_v2,
)
from zarr.core.dtype.npy.common import (
    ComplexLike,
    TComplexDType_co,
    TComplexScalar_co,
    check_json_complex_float_v2,
    check_json_complex_float_v3,
    complex_float_from_json_v2,
    complex_float_from_json_v3,
    complex_float_to_json_v2,
    complex_float_to_json_v3,
    endianness_to_numpy_str,
    get_endianness_from_numpy_dtype,
)
from zarr.core.dtype.wrapper import TBaseDType, ZDType

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat


@dataclass(frozen=True)
class BaseComplex(ZDType[TComplexDType_co, TComplexScalar_co], HasEndianness, HasItemSize):
    """
    A base class for Zarr data types that wrap NumPy complex float data types.
    """

    # This attribute holds the possible zarr v2 JSON names for the data type
    _zarr_v2_names: ClassVar[tuple[str, ...]]

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of this data type from a NumPy complex dtype.

        Parameters
        ----------
        dtype : TBaseDType
            The native dtype to convert.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the dtype is not compatible with this data type.
        """
        if cls._check_native_dtype(dtype):
            return cls(endianness=get_endianness_from_numpy_dtype(dtype))
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> TComplexDType_co:
        """
        Convert this class to a NumPy complex dtype with the appropriate byte order.

        Returns
        -------
        TComplexDType_co
            A NumPy data type object representing the complex data type with the specified byte order.
        """

        byte_order = endianness_to_numpy_str(self.endianness)
        return self.dtype_cls().newbyteorder(byte_order)  # type: ignore[return-value]

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[DTypeConfig_V2[str, None]]:
        """
        Check that the input is a valid JSON representation of this data type.

        The input data must be a mapping that contains a "name" key that is one of
        the strings from cls._zarr_v2_names and an "object_codec_id" key that is None.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        bool
            True if the input is a valid JSON representation, False otherwise.
        """
        return (
            check_dtype_spec_v2(data)
            and data["name"] in cls._zarr_v2_names
            and data["object_codec_id"] is None
        )

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[str]:
        """
        Check that the input is a valid JSON representation of this data type in Zarr V3.

        This method verifies that the provided data matches the expected Zarr V3
        representation, which is the string specified by the class-level attribute _zarr_v3_name.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[str]
            True if the input is a valid representation of this class in Zarr V3, False otherwise.
        """

        return data == cls._zarr_v3_name

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from Zarr V2-flavored JSON.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Self
            An instance of this class.

        Raises
        ------
        DataTypeValidationError
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v2(data):
            # Going via numpy ensures that we get the endianness correct without
            # annoying string parsing.
            name = data["name"]
            return cls.from_native_dtype(np.dtype(name))
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected one of the strings {cls._zarr_v2_names}."
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this class from Zarr V3-flavored JSON.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v3(data):
            return cls()
        msg = f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected {cls._zarr_v3_name}."
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> DTypeConfig_V2[str, None]: ...

    @overload
    def to_json(self, zarr_format: Literal[3]) -> str: ...

    def to_json(self, zarr_format: ZarrFormat) -> DTypeConfig_V2[str, None] | str:
        """
        Serialize this object to a JSON-serializable representation.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The Zarr format version. Supported values are 2 and 3.

        Returns
        -------
        DTypeConfig_V2[str, None] | str
            If ``zarr_format`` is 2, a dictionary with ``"name"`` and ``"object_codec_id"`` keys is
            returned.
            If ``zarr_format`` is 3, a string representation of the complex data type is returned.

        Raises
        ------
        ValueError
            If `zarr_format` is not 2 or 3.
        """

        if zarr_format == 2:
            return {"name": self.to_native_dtype().str, "object_codec_id": None}
        elif zarr_format == 3:
            return self._zarr_v3_name
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[ComplexLike]:
        """
        Check that the input is a scalar complex value.

        Parameters
        ----------
        data : object
            The value to check.

        Returns
        -------
        TypeGuard[ComplexLike]
            True if the input is a scalar complex value, False otherwise.
        """
        return isinstance(data, ComplexLike)

    def _cast_scalar_unchecked(self, data: ComplexLike) -> TComplexScalar_co:
        """
        Cast the provided scalar data to the native scalar type of this class.

        Parameters
        ----------
        data : ComplexLike
            The data to cast.

        Returns
        -------
        TComplexScalar_co
            The casted data as a numpy complex scalar.

        Notes
        -----
        This method does not perform any type checking.
        The input data must be a scalar complex value.
        """
        return self.to_native_dtype().type(data)  # type: ignore[return-value]

    def cast_scalar(self, data: object) -> TComplexScalar_co:
        """
        Attempt to cast a given object to a numpy complex scalar.

        Parameters
        ----------
        data : object
            The data to be cast to a numpy complex scalar.

        Returns
        -------
        TComplexScalar_co
            The data cast as a numpy complex scalar.

        Raises
        ------
        TypeError
            If the data cannot be converted to a numpy complex scalar.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)

    def default_scalar(self) -> TComplexScalar_co:
        """
        Get the default value, which is 0 cast to this dtype

        Returns
        -------
        Int scalar
            The default value.
        """
        return self._cast_scalar_unchecked(0)

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> TComplexScalar_co:
        """
        Read a JSON-serializable value as a numpy float.

        Parameters
        ----------
        data : JSON
            The JSON-serializable value.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        TScalar_co
            The numpy float.
        """
        if zarr_format == 2:
            if check_json_complex_float_v2(data):
                return self._cast_scalar_unchecked(complex_float_from_json_v2(data))
            raise TypeError(
                f"Invalid type: {data}. Expected a float or a special string encoding of a float."
            )
        elif zarr_format == 3:
            if check_json_complex_float_v3(data):
                return self._cast_scalar_unchecked(complex_float_from_json_v3(data))
            raise TypeError(
                f"Invalid type: {data}. Expected a float or a special string encoding of a float."
            )
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> JSON:
        """
        Convert an object to a JSON-serializable float.

        Parameters
        ----------
        data : _BaseScalar
            The value to convert.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        JSON
            The JSON-serializable form of the complex number, which is a list of two floats,
            each of which is encoding according to a zarr-format-specific encoding.
        """
        if zarr_format == 2:
            return complex_float_to_json_v2(self.cast_scalar(data))
        elif zarr_format == 3:
            return complex_float_to_json_v3(self.cast_scalar(data))
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover


@dataclass(frozen=True, kw_only=True)
class Complex64(BaseComplex[np.dtypes.Complex64DType, np.complex64]):
    """
    A Zarr data type for arrays containing 64 bit complex floats.

    Wraps the [`np.dtypes.Complex64DType`][numpy.dtypes.Complex64DType] data type. Scalars for this data type
    are instances of [`np.complex64`][numpy.complex64].

    Attributes
    ----------
    dtype_cls : Type[np.dtypes.Complex64DType]
        The numpy dtype class for this data type.
    _zarr_v3_name : ClassVar[Literal["complex64"]]
        The name of this data type in Zarr V3.
    _zarr_v2_names : ClassVar[tuple[Literal[">c8"], Literal["<c8"]]]
        The names of this data type in Zarr V2.
    """

    dtype_cls = np.dtypes.Complex64DType
    _zarr_v3_name: ClassVar[Literal["complex64"]] = "complex64"
    _zarr_v2_names: ClassVar[tuple[Literal[">c8"], Literal["<c8"]]] = (">c8", "<c8")

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return 8


@dataclass(frozen=True, kw_only=True)
class Complex128(BaseComplex[np.dtypes.Complex128DType, np.complex128], HasEndianness):
    """
    A Zarr data type for arrays containing 64 bit complex floats.

    Wraps the [`np.dtypes.Complex128DType`][numpy.dtypes.Complex128DType] data type. Scalars for this data type
    are instances of [`np.complex128`][numpy.complex128].

    Attributes
    ----------
    dtype_cls : Type[np.dtypes.Complex128DType]
        The numpy dtype class for this data type.
    _zarr_v3_name : ClassVar[Literal["complex128"]]
        The name of this data type in Zarr V3.
    _zarr_v2_names : ClassVar[tuple[Literal[">c16"], Literal["<c16"]]]
        The names of this data type in Zarr V2.
    """

    dtype_cls = np.dtypes.Complex128DType
    _zarr_v3_name: ClassVar[Literal["complex128"]] = "complex128"
    _zarr_v2_names: ClassVar[tuple[Literal[">c16"], Literal["<c16"]]] = (">c16", "<c16")

    @property
    def item_size(self) -> int:
        """
        The size of a single scalar in bytes.

        Returns
        -------
        int
            The size of a single scalar in bytes.
        """
        return 16
