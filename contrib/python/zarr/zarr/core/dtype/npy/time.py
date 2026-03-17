from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Literal,
    Self,
    TypedDict,
    TypeGuard,
    TypeVar,
    cast,
    get_args,
    overload,
)

import numpy as np
from typing_extensions import ReadOnly

from zarr.core.common import NamedConfig
from zarr.core.dtype.common import (
    DataTypeValidationError,
    DTypeConfig_V2,
    DTypeJSON,
    HasEndianness,
    HasItemSize,
    check_dtype_spec_v2,
)
from zarr.core.dtype.npy.common import (
    DATETIME_UNIT,
    DateTimeUnit,
    check_json_int,
    endianness_to_numpy_str,
    get_endianness_from_numpy_dtype,
)
from zarr.core.dtype.wrapper import TBaseDType, ZDType

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat

TimeDeltaLike = str | int | bytes | np.timedelta64 | timedelta | None
DateTimeLike = str | int | bytes | np.datetime64 | datetime | None


def datetime_from_int(data: int, *, unit: DateTimeUnit, scale_factor: int) -> np.datetime64:
    """
    Convert an integer to a datetime64.

    Parameters
    ----------
    data : int
        The integer to convert.
    unit : DateTimeUnit
        The unit of the datetime64.
    scale_factor : int
        The scale factor of the datetime64.

    Returns
    -------
    numpy.datetime64
        The datetime64 value.
    """
    dtype_name = f"datetime64[{scale_factor}{unit}]"
    return cast("np.datetime64", np.int64(data).view(dtype_name))


def datetimelike_to_int(data: np.datetime64 | np.timedelta64) -> int:
    """
    Convert a datetime64 or a timedelta64 to an integer.

    Parameters
    ----------
    data : np.datetime64 | numpy.timedelta64
        The value to convert.

    Returns
    -------
    int
        An integer representation of the scalar.
    """
    return data.view(np.int64).item()


def check_json_time(data: JSON) -> TypeGuard[Literal["NaT"] | int]:
    """
    Type guard to check if the input JSON data is the literal string "NaT"
    or an integer.
    """
    return check_json_int(data) or data == "NaT"


BaseTimeDType_co = TypeVar(
    "BaseTimeDType_co",
    bound=np.dtypes.TimeDelta64DType | np.dtypes.DateTime64DType,
    covariant=True,
)
BaseTimeScalar_co = TypeVar(
    "BaseTimeScalar_co", bound=np.timedelta64 | np.datetime64, covariant=True
)


class TimeConfig(TypedDict):
    """
    The configuration for the numpy.timedelta64 or numpy.datetime64 data type in Zarr V3.

    Attributes
    ----------
    unit : ReadOnly[DateTimeUnit]
        A string encoding a unit of time.
    scale_factor : ReadOnly[int]
        A scale factor.

    Examples
    --------
    ```python
    {"unit": "ms", "scale_factor": 1}
    ```
    """

    unit: ReadOnly[DateTimeUnit]
    scale_factor: ReadOnly[int]


class DateTime64JSON_V3(NamedConfig[Literal["numpy.datetime64"], TimeConfig]):
    """
    The JSON representation of the ``numpy.datetime64`` data type in Zarr V3.

    References
    ----------
    This representation is defined in the ``numpy.datetime64``
    [specification document](https://zarr-specs.readthedocs.io/en/latest/spec/v3/datatypes.html#numpy-datetime64).

    Examples
    --------
    ```python
    {
        "name": "numpy.datetime64",
        "configuration": {
            "unit": "ms",
            "scale_factor": 1
            }
    }
    ```
    """


class TimeDelta64JSON_V3(NamedConfig[Literal["numpy.timedelta64"], TimeConfig]):
    """
    The JSON representation of the ``TimeDelta64`` data type in Zarr V3.

    References
    ----------
    This representation is defined in the numpy.timedelta64
    [specification document](https://zarr-specs.readthedocs.io/en/latest/spec/v3/datatypes.html#numpy-timedelta64).

    Examples
    --------
    ```python
    {
        "name": "numpy.timedelta64",
        "configuration": {
            "unit": "ms",
            "scale_factor": 1
            }
    }
    ```
    """


class TimeDelta64JSON_V2(DTypeConfig_V2[str, None]):
    """
    A wrapper around the JSON representation of the ``TimeDelta64`` data type in Zarr V2.

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
        "name": "<m8[1s]",
        "object_codec_id": None
    }
    ```
    """


class DateTime64JSON_V2(DTypeConfig_V2[str, None]):
    """
    A wrapper around the JSON representation of the ``DateTime64`` data type in Zarr V2.

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
        "name": "<M8[10s]",
        "object_codec_id": None
    }
    ```
    """


@dataclass(frozen=True, kw_only=True, slots=True)
class TimeDTypeBase(ZDType[BaseTimeDType_co, BaseTimeScalar_co], HasEndianness, HasItemSize):
    """
    A base class for data types that represent time via the NumPy TimeDelta64 and DateTime64 data
    types.

    Attributes
    ----------
    scale_factor : int
        The scale factor for the time unit.
    unit : str
        The unit of time.
    """

    _numpy_name: ClassVar[Literal["datetime64", "timedelta64"]]
    scale_factor: int
    unit: DateTimeUnit

    def __post_init__(self) -> None:
        if self.scale_factor < 1:
            raise ValueError(f"scale_factor must be > 0, got {self.scale_factor}.")
        if self.scale_factor >= 2**31:
            raise ValueError(f"scale_factor must be < 2147483648, got {self.scale_factor}.")
        if self.unit not in get_args(DateTimeUnit):
            raise ValueError(f"unit must be one of {get_args(DateTimeUnit)}, got {self.unit!r}.")

    @classmethod
    def from_native_dtype(cls, dtype: TBaseDType) -> Self:
        """
        Create an instance of this class from a native NumPy data type.

        Parameters
        ----------
        dtype : TBaseDType
            The native NumPy dtype to convert.

        Returns
        -------
        Self
            An instance of this data type.

        Raises
        ------
        DataTypeValidationError
            If the dtype is not a valid representation of this class.
        """

        if cls._check_native_dtype(dtype):
            unit, scale_factor = np.datetime_data(dtype.name)
            unit = cast("DateTimeUnit", unit)
            return cls(
                unit=unit,
                scale_factor=scale_factor,
                endianness=get_endianness_from_numpy_dtype(dtype),
            )
        raise DataTypeValidationError(
            f"Invalid data type: {dtype}. Expected an instance of {cls.dtype_cls}"
        )

    def to_native_dtype(self) -> BaseTimeDType_co:
        # Numpy does not allow creating datetime64 or timedelta64 via
        # np.dtypes.{dtype_name}()
        # so we use np.dtype with a formatted string.
        """
        Convert this data type to a NumPy temporal data type with the appropriate
        unit and scale factor.

        Returns
        -------
        BaseTimeDType_co
            A NumPy data type object representing the time data type with
            the specified unit, scale factor, and byte order.
        """

        dtype_string = f"{self._numpy_name}[{self.scale_factor}{self.unit}]"
        return np.dtype(dtype_string).newbyteorder(endianness_to_numpy_str(self.endianness))  # type: ignore[return-value]

    def to_json_scalar(self, data: object, *, zarr_format: ZarrFormat) -> int:
        """
        Convert a python object to a JSON representation of a datetime64 or timedelta64 scalar.

        Parameters
        ----------
        data : object
            The python object to convert.
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3).

        Returns
        -------
        int
            The JSON representation of the scalar.
        """
        return datetimelike_to_int(data)  # type: ignore[arg-type]

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


@dataclass(frozen=True, kw_only=True, slots=True)
class TimeDelta64(TimeDTypeBase[np.dtypes.TimeDelta64DType, np.timedelta64], HasEndianness):
    """
    A Zarr data type for arrays containing NumPy TimeDelta64 data.

    Wraps the ``np.dtypesTimeDelta64DType`` data type. Scalars for this data type
    are instances of `np.timedelta64`.

    Attributes
    ----------
    dtype_cls : Type[np.dtypesTimeDelta64DType]
        The NumPy dtype class for this data type.
    scale_factor : int
        The scale factor for this data type.
    unit : DateTimeUnit
        The unit for this data type.

    References
    ----------
    The Zarr V2 representation of this data type is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).

    The Zarr V3 representation of this data type is defined in the ``numpy.timedelta64``
    [specification document](https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/numpy.timedelta64)
    """

    # mypy infers the type of np.dtypes.TimeDelta64DType to be
    # "Callable[[Literal['Y', 'M', 'W', 'D'] | Literal['h', 'm', 's', 'ms', 'us', 'ns', 'ps', 'fs', 'as']], Never]"
    dtype_cls = np.dtypes.TimeDelta64DType  # type: ignore[assignment]
    unit: DateTimeUnit = "generic"
    scale_factor: int = 1
    _zarr_v3_name: ClassVar[Literal["numpy.timedelta64"]] = "numpy.timedelta64"
    _zarr_v2_names: ClassVar[tuple[Literal[">m8"], Literal["<m8"]]] = (">m8", "<m8")
    _numpy_name: ClassVar[Literal["timedelta64"]] = "timedelta64"

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[TimeDelta64JSON_V2]:
        """
        Validate that the provided JSON input accurately represents a NumPy timedelta64 data type,
        which could be in the form of strings like "<m8" or ">m8[10s]". This method serves as a type
        guard, helping to refine the type of unknown JSON input by confirming its adherence to the
        expected format for NumPy timedelta64 data types.

        The JSON input should contain a "name" key with a value that matches the expected string
        pattern for NumPy timedelta64 data types. The pattern includes an optional unit enclosed
        within square brackets, following the base type identifier.

        Returns
        -------
        bool
            True if the JSON input is a valid representation of this class,
            otherwise False.
        """
        if not check_dtype_spec_v2(data):
            return False
        name = data["name"]
        # match <m[ns], >m[M], etc
        # consider making this a standalone function
        if not isinstance(name, str):
            return False
        if not name.startswith(cls._zarr_v2_names):
            return False
        if len(name) == 3:
            # no unit, and
            # we already checked that this string is either <m8 or >m8
            return True
        else:
            return name[4:-1].endswith(DATETIME_UNIT) and name[-1] == "]"

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[DateTime64JSON_V3]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Returns
        -------
        TypeGuard[DateTime64JSON_V3]
            True if the JSON input is a valid representation of this class,
            otherwise False.
        """
        return (
            isinstance(data, dict)
            and set(data.keys()) == {"name", "configuration"}
            and data["name"] == cls._zarr_v3_name
            and isinstance(data["configuration"], dict)
            and set(data["configuration"].keys()) == {"unit", "scale_factor"}
        )

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create a TimeDelta64 from a Zarr V2-flavored JSON.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data.

        Returns
        -------
        TimeDelta64
            An instance of TimeDelta64.

        Raises
        ------
        DataTypeValidationError
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v2(data):
            name = data["name"]
            return cls.from_native_dtype(np.dtype(name))
        msg = (
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a string "
            f"representation of an instance of {cls.dtype_cls}"
        )
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create a TimeDelta64 from a Zarr V3-flavored JSON.

        The JSON representation of a TimeDelta64 in Zarr V3 is a dict with a 'name' key
        with the value 'numpy.timedelta64', and a 'configuration' key with a value of a dict
        with a 'unit' key and a 'scale_factor' key.

        For example:

        ```json
        {
            "name": "numpy.timedelta64",
            "configuration": {
                "unit": "generic",
                "scale_factor": 1
            }
        }
        ```

        """
        if cls._check_json_v3(data):
            unit = data["configuration"]["unit"]
            scale_factor = data["configuration"]["scale_factor"]
            return cls(unit=unit, scale_factor=scale_factor)
        msg = (
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a dict "
            f"with a 'name' key with the value 'numpy.timedelta64', "
            "and a 'configuration' key with a value of a dict with a 'unit' key and a "
            "'scale_factor' key"
        )
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> TimeDelta64JSON_V2: ...
    @overload
    def to_json(self, zarr_format: Literal[3]) -> TimeDelta64JSON_V3: ...

    def to_json(self, zarr_format: ZarrFormat) -> TimeDelta64JSON_V2 | TimeDelta64JSON_V3:
        """
        Serialize this data type to JSON.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3).

        Returns
        -------
        TimeDelta64JSON_V2 | TimeDelta64JSON_V3
            The JSON representation of the data type.

        Raises
        ------
        ValueError
            If the zarr_format is not 2 or 3.
        """
        if zarr_format == 2:
            name = self.to_native_dtype().str
            return {"name": name, "object_codec_id": None}
        elif zarr_format == 3:
            return {
                "name": self._zarr_v3_name,
                "configuration": {"unit": self.unit, "scale_factor": self.scale_factor},
            }
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[TimeDeltaLike]:
        """
        Check if the input is a scalar of this data type.

        Parameters
        ----------
        data : object
            The object to check.

        Returns
        -------
        TypeGuard[TimeDeltaLike]
            True if the input is a scalar of this data type, False otherwise.
        """
        if data is None:
            return True
        return isinstance(data, str | int | bytes | np.timedelta64 | timedelta)

    def _cast_scalar_unchecked(self, data: TimeDeltaLike) -> np.timedelta64:
        """
        Cast the provided scalar input to a numpy timedelta64 without any type checking.

        This method assumes that the input data is already a valid scalar of this data type,
        and does not perform any validation or type checks. It directly casts the input
        to a numpy timedelta64 scalar using the unit and scale factor defined in the class.

        Parameters
        ----------
        data : TimeDeltaLike
            The scalar input data to cast.

        Returns
        -------
        numpy.timedelta64
            The input data cast as a numpy timedelta64 scalar.
        """
        return self.to_native_dtype().type(data, f"{self.scale_factor}{self.unit}")

    def cast_scalar(self, data: object) -> np.timedelta64:
        """
        Cast the input to a numpy timedelta64 scalar. If the input is not a scalar of this data type,
        raise a TypeError.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)

    def default_scalar(self) -> np.timedelta64:
        """
        Return a default scalar of this data type.

        This method provides a default value for the timedelta64 scalar, which is
        a 'Not-a-Time' (NaT) value.
        """
        return np.timedelta64("NaT")

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.timedelta64:
        """
        Create a scalar of this data type from JSON input.

        Parameters
        ----------
        data : JSON
            The JSON representation of the scalar value.
        zarr_format : int
            The zarr format to use for the JSON representation.

        Returns
        -------
        numpy.timedelta64
            The scalar value of this data type.

        Raises
        ------
        TypeError
            If the input JSON is not a valid representation of a scalar for this data type.
        """
        if check_json_time(data):
            return self.to_native_dtype().type(data, f"{self.scale_factor}{self.unit}")
        raise TypeError(f"Invalid type: {data}. Expected an integer.")  # pragma: no cover


@dataclass(frozen=True, kw_only=True, slots=True)
class DateTime64(TimeDTypeBase[np.dtypes.DateTime64DType, np.datetime64], HasEndianness):
    """
    A Zarr data type for arrays containing NumPy Datetime64 data.

    Wraps the ``np.dtypes.TimeDelta64DType`` data type. Scalars for this data type
    are instances of ``np.datetime64``.

    Attributes
    ----------
    dtype_cls : Type[np.dtypesTimeDelta64DType]
        The numpy dtype class for this data type.
    unit : DateTimeUnit
        The unit of time for this data type.
    scale_factor : int
        The scale factor for the time unit.

    References
    ----------
    The Zarr V2 representation of this data type is defined in the Zarr V2
    [specification document](https://github.com/zarr-developers/zarr-specs/blob/main/docs/v2/v2.0.rst#data-type-encoding).

    The Zarr V3 representation of this data type is defined in the ``numpy.datetime64``
    [specification document](https://github.com/zarr-developers/zarr-extensions/tree/main/data-types/numpy.datetime64)
    """

    dtype_cls = np.dtypes.DateTime64DType  # type: ignore[assignment]
    _zarr_v3_name: ClassVar[Literal["numpy.datetime64"]] = "numpy.datetime64"
    _zarr_v2_names: ClassVar[tuple[Literal[">M8"], Literal["<M8"]]] = (">M8", "<M8")
    _numpy_name: ClassVar[Literal["datetime64"]] = "datetime64"
    unit: DateTimeUnit = "generic"
    scale_factor: int = 1

    @classmethod
    def _check_json_v2(cls, data: DTypeJSON) -> TypeGuard[DateTime64JSON_V2]:
        """
        Check that the input is a valid JSON representation of this data type.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[DateTime64JSON_V2]
            True if the input is a valid JSON representation of a NumPy datetime64 data type,
            otherwise False.
        """
        if not check_dtype_spec_v2(data):
            return False
        name = data["name"]
        if not isinstance(name, str):
            return False
        if not name.startswith(cls._zarr_v2_names):
            return False
        if len(name) == 3:
            # no unit, and
            # we already checked that this string is either <M8 or >M8
            return True
        else:
            return name[4:-1].endswith(DATETIME_UNIT) and name[-1] == "]"

    @classmethod
    def _check_json_v3(cls, data: DTypeJSON) -> TypeGuard[DateTime64JSON_V3]:
        """
        Check that the input is a valid JSON representation of this class in Zarr V3.

        Parameters
        ----------
        data : DTypeJSON
            The JSON data to check.

        Returns
        -------
        TypeGuard[DateTime64JSON_V3]
            True if the input is a valid JSON representation of a numpy datetime64 data type in Zarr V3, False otherwise.
        """

        return (
            isinstance(data, dict)
            and set(data.keys()) == {"name", "configuration"}
            and data["name"] == cls._zarr_v3_name
            and isinstance(data["configuration"], dict)
            and set(data["configuration"].keys()) == {"unit", "scale_factor"}
        )

    @classmethod
    def _from_json_v2(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this data type from a Zarr V2-flavored JSON representation.

        This method checks if the provided JSON data is a valid representation of this class.
        If valid, it creates an instance using the native NumPy dtype. Otherwise, it raises a
        DataTypeValidationError.

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
            If the input JSON is not a valid representation of this class.
        """

        if cls._check_json_v2(data):
            name = data["name"]
            return cls.from_native_dtype(np.dtype(name))
        msg = (
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a string "
            f"representation of an instance of {cls.dtype_cls}"
        )
        raise DataTypeValidationError(msg)

    @classmethod
    def _from_json_v3(cls, data: DTypeJSON) -> Self:
        """
        Create an instance of this data type from a Zarr V3-flavored JSON representation.

        This method checks if the provided JSON data is a valid representation of this class.
        If valid, it creates an instance using the native NumPy dtype. Otherwise, it raises a
        DataTypeValidationError.

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
            If the input JSON is not a valid representation of this class.
        """
        if cls._check_json_v3(data):
            unit = data["configuration"]["unit"]
            scale_factor = data["configuration"]["scale_factor"]
            return cls(unit=unit, scale_factor=scale_factor)
        msg = (
            f"Invalid JSON representation of {cls.__name__}. Got {data!r}, expected a dict "
            f"with a 'name' key with the value 'numpy.datetime64', "
            "and a 'configuration' key with a value of a dict with a 'unit' key and a "
            "'scale_factor' key"
        )
        raise DataTypeValidationError(msg)

    @overload
    def to_json(self, zarr_format: Literal[2]) -> DateTime64JSON_V2: ...
    @overload
    def to_json(self, zarr_format: Literal[3]) -> DateTime64JSON_V3: ...

    def to_json(self, zarr_format: ZarrFormat) -> DateTime64JSON_V2 | DateTime64JSON_V3:
        """
        Serialize this data type to JSON.

        Parameters
        ----------
        zarr_format : ZarrFormat
            The Zarr format version (2 or 3).

        Returns
        -------
        DateTime64JSON_V2 | DateTime64JSON_V3
            The JSON representation of the data type.

        Raises
        ------
        ValueError
            If the zarr_format is not 2 or 3.
        """
        if zarr_format == 2:
            name = self.to_native_dtype().str
            return {"name": name, "object_codec_id": None}
        elif zarr_format == 3:
            return {
                "name": self._zarr_v3_name,
                "configuration": {"unit": self.unit, "scale_factor": self.scale_factor},
            }
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format}")  # pragma: no cover

    def _check_scalar(self, data: object) -> TypeGuard[DateTimeLike]:
        """
        Check if the input is convertible to a scalar of this data type.

        Parameters
        ----------
        data : object
            The object to check.

        Returns
        -------
        TypeGuard[DateTimeLike]
            True if the input is a scalar of this data type, False otherwise.
        """
        if data is None:
            return True
        return isinstance(data, str | int | bytes | np.datetime64 | datetime)

    def _cast_scalar_unchecked(self, data: DateTimeLike) -> np.datetime64:
        """
        Cast the input to a scalar of this data type without any type checking.

        Parameters
        ----------
        data : DateTimeLike
            The scalar data to cast.

        Returns
        -------
        numpy.datetime64
            The input cast to a NumPy datetime scalar.
        """
        return self.to_native_dtype().type(data, f"{self.scale_factor}{self.unit}")

    def cast_scalar(self, data: object) -> np.datetime64:
        """
        Cast the input to a scalar of this data type after a type check.

        Parameters
        ----------
        data : object
            The scalar value to cast.

        Returns
        -------
        numpy.datetime64
            The input cast to a NumPy datetime scalar.

        Raises
        ------
        TypeError
            If the data cannot be converted to a numpy datetime scalar.
        """
        if self._check_scalar(data):
            return self._cast_scalar_unchecked(data)
        msg = (
            f"Cannot convert object {data!r} with type {type(data)} to a scalar compatible with the "
            f"data type {self}."
        )
        raise TypeError(msg)

    def default_scalar(self) -> np.datetime64:
        """
        Return the default scalar value for this data type.

        Returns
        -------
        numpy.datetime64
            The default scalar value, which is a 'Not-a-Time' (NaT) value
        """

        return np.datetime64("NaT")

    def from_json_scalar(self, data: JSON, *, zarr_format: ZarrFormat) -> np.datetime64:
        """
        Read a JSON-serializable value as a scalar.

        Parameters
        ----------
        data : JSON
            The JSON-serializable value.
        zarr_format : ZarrFormat
            The zarr format version.

        Returns
        -------
        numpy.datetime64
            The numpy datetime scalar.

        Raises
        ------
        TypeError
            If the input is not a valid integer type.
        """
        if check_json_time(data):
            return self._cast_scalar_unchecked(data)
        raise TypeError(f"Invalid type: {data}. Expected an integer.")  # pragma: no cover
