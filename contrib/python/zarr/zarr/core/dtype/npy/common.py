from __future__ import annotations

import base64
import struct
import sys
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    NewType,
    SupportsComplex,
    SupportsFloat,
    SupportsIndex,
    SupportsInt,
    TypeGuard,
    TypeVar,
)

import numpy as np

from zarr.core.dtype.common import (
    ENDIANNESS_STR,
    SPECIAL_FLOAT_STRINGS,
    EndiannessStr,
    JSONFloatV2,
    JSONFloatV3,
)

if TYPE_CHECKING:
    from zarr.core.common import JSON, ZarrFormat

IntLike = SupportsInt | SupportsIndex | bytes | str
FloatLike = SupportsIndex | SupportsFloat | bytes | str
ComplexLike = SupportsFloat | SupportsIndex | SupportsComplex | bytes | str | None
DateTimeUnit = Literal[
    "Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as", "generic"
]
DATETIME_UNIT: Final = (
    "Y",
    "M",
    "W",
    "D",
    "h",
    "m",
    "s",
    "ms",
    "us",
    "μs",
    "ns",
    "ps",
    "fs",
    "as",
    "generic",
)

IntishFloat = NewType("IntishFloat", float)
"""A type for floats that represent integers, like 1.0 (but not 1.1)."""

IntishStr = NewType("IntishStr", str)
"""A type for strings that represent integers, like "0" or "42"."""

FloatishStr = NewType("FloatishStr", str)
"""A type for strings that represent floats, like "3.14" or "-2.5"."""

NumpyEndiannessStr = Literal[">", "<", "="]
NUMPY_ENDIANNESS_STR: Final = ">", "<", "="

TFloatDType_co = TypeVar(
    "TFloatDType_co",
    bound=np.dtypes.Float16DType | np.dtypes.Float32DType | np.dtypes.Float64DType,
    covariant=True,
)
TFloatScalar_co = TypeVar(
    "TFloatScalar_co", bound=np.float16 | np.float32 | np.float64, covariant=True
)

TComplexDType_co = TypeVar(
    "TComplexDType_co", bound=np.dtypes.Complex64DType | np.dtypes.Complex128DType, covariant=True
)
TComplexScalar_co = TypeVar("TComplexScalar_co", bound=np.complex64 | np.complex128, covariant=True)


def endianness_from_numpy_str(endianness: NumpyEndiannessStr) -> EndiannessStr:
    """
    Convert a numpy endianness string literal to a human-readable literal value.

    Parameters
    ----------
    endianness : Literal[">", "<", "="]
        The numpy string representation of the endianness.

    Returns
    -------
    Endianness
        The human-readable representation of the endianness.

    Raises
    ------
    ValueError
        If the endianness is invalid.
    """
    match endianness:
        case "=":
            # Use the local system endianness
            return sys.byteorder
        case "<":
            return "little"
        case ">":
            return "big"
    raise ValueError(f"Invalid endianness: {endianness!r}. Expected one of {NUMPY_ENDIANNESS_STR}")


def endianness_to_numpy_str(endianness: EndiannessStr) -> NumpyEndiannessStr:
    """
    Convert an endianness literal to its numpy string representation.

    Parameters
    ----------
    endianness : Endianness
        The endianness to convert.

    Returns
    -------
    Literal[">", "<"]
        The numpy string representation of the endianness.

    Raises
    ------
    ValueError
        If the endianness is invalid.
    """
    match endianness:
        case "little":
            return "<"
        case "big":
            return ">"
    raise ValueError(
        f"Invalid endianness: {endianness!r}. Expected one of {ENDIANNESS_STR} or None"
    )


def get_endianness_from_numpy_dtype(dtype: np.dtype[np.generic]) -> EndiannessStr:
    """
    Gets the endianness from a numpy dtype that has an endianness. This function will
    raise a ValueError if the numpy data type does not have a concrete endianness.
    """
    endianness = dtype.byteorder
    if dtype.byteorder in NUMPY_ENDIANNESS_STR:
        return endianness_from_numpy_str(endianness)  # type: ignore [arg-type]
    raise ValueError(f"The dtype {dtype} has an unsupported endianness: {endianness}")


def float_from_json_v2(data: JSONFloatV2) -> float:
    """
    Convert a JSON float to a float (Zarr v2).

    Parameters
    ----------
    data : JSONFloat
        The JSON float to convert.

    Returns
    -------
    float
        The float value.
    """
    match data:
        case "NaN":
            return float("nan")
        case "Infinity":
            return float("inf")
        case "-Infinity":
            return float("-inf")
        case _:
            return float(data)


def float_from_json_v3(data: JSONFloatV3) -> float:
    """
    Convert a JSON float to a float (v3).

    Parameters
    ----------
    data : JSONFloat
        The JSON float to convert.

    Returns
    -------
    float
        The float value.

    Notes
    -----
    Zarr V3 allows floats to be stored as hex strings. To quote the spec:
       "...for float32, "NaN" is equivalent to "0x7fc00000".
       This representation is the only way to specify a NaN value other than the specific NaN value
       denoted by "NaN"."
    """

    if isinstance(data, str):
        if data in SPECIAL_FLOAT_STRINGS:
            return float_from_json_v2(data)  # type: ignore[arg-type]
        if not data.startswith("0x"):
            msg = (
                f"Invalid float value: {data!r}. Expected a string starting with the hex prefix"
                " '0x', or one of 'NaN', 'Infinity', or '-Infinity'."
            )
            raise ValueError(msg)
        if len(data[2:]) == 4:
            dtype_code = ">e"
        elif len(data[2:]) == 8:
            dtype_code = ">f"
        elif len(data[2:]) == 16:
            dtype_code = ">d"
        else:
            msg = (
                f"Invalid hexadecimal float value: {data!r}. "
                "Expected the '0x' prefix to be followed by 4, 8, or 16 numeral characters"
            )
            raise ValueError(msg)
        return float(struct.unpack(dtype_code, bytes.fromhex(data[2:]))[0])
    return float_from_json_v2(data)


def bytes_from_json(data: str, *, zarr_format: ZarrFormat) -> bytes:
    """
    Convert a JSON string to bytes

    Parameters
    ----------
    data : str
        The JSON string to convert.
    zarr_format : ZarrFormat
        The zarr format version.

    Returns
    -------
    bytes
        The bytes.
    """
    if zarr_format == 2:
        return base64.b64decode(data.encode("ascii"))
    # TODO: differentiate these as needed. This is a spec question.
    if zarr_format == 3:
        return base64.b64decode(data.encode("ascii"))
    raise ValueError(f"Invalid zarr format: {zarr_format}. Expected 2 or 3.")  # pragma: no cover


def bytes_to_json(data: bytes, zarr_format: ZarrFormat) -> str:
    """
    Convert bytes to JSON.

    Parameters
    ----------
    data : bytes
        The bytes to store.
    zarr_format : ZarrFormat
        The zarr format version.

    Returns
    -------
    str
        The bytes encoded as ascii using the base64 alphabet.
    """
    # TODO: decide if we are going to make this implementation zarr format-specific
    return base64.b64encode(data).decode("ascii")


def float_to_json_v2(data: float | np.floating[Any]) -> JSONFloatV2:
    """
    Convert a float to JSON (v2).

    Parameters
    ----------
    data : float or np.floating
        The float value to convert.

    Returns
    -------
    JSONFloat
        The JSON representation of the float.
    """
    if np.isnan(data):
        return "NaN"
    elif np.isinf(data):
        return "Infinity" if data > 0 else "-Infinity"
    return float(data)


def float_to_json_v3(data: float | np.floating[Any]) -> JSONFloatV3:
    """
    Convert a float to JSON (v3).

    Parameters
    ----------
    data : float or np.floating
        The float value to convert.

    Returns
    -------
    JSONFloat
        The JSON representation of the float.
    """
    # v3 can in principle handle distinct NaN values, but numpy does not represent these explicitly
    # so we just reuse the v2 routine here
    return float_to_json_v2(data)


def complex_float_to_json_v3(
    data: complex | np.complexfloating[Any, Any],
) -> tuple[JSONFloatV3, JSONFloatV3]:
    """
    Convert a complex number to JSON as defined by the Zarr V3 spec.

    Parameters
    ----------
    data : complex or np.complexfloating
        The complex value to convert.

    Returns
    -------
    tuple[JSONFloat, JSONFloat]
        The JSON representation of the complex number.
    """
    return float_to_json_v3(data.real), float_to_json_v3(data.imag)


def complex_float_to_json_v2(
    data: complex | np.complexfloating[Any, Any],
) -> tuple[JSONFloatV2, JSONFloatV2]:
    """
    Convert a complex number to JSON as defined by the Zarr V2 spec.

    Parameters
    ----------
    data : complex | np.complexfloating
        The complex value to convert.

    Returns
    -------
    tuple[JSONFloat, JSONFloat]
        The JSON representation of the complex number.
    """
    return float_to_json_v2(data.real), float_to_json_v2(data.imag)


def complex_float_from_json_v2(data: tuple[JSONFloatV2, JSONFloatV2]) -> complex:
    """
    Convert a JSON complex float to a complex number (v2).

    Parameters
    ----------
    data : tuple[JSONFloat, JSONFloat]
        The JSON complex float to convert.

    Returns
    -------
    np.complexfloating
        The complex number.
    """
    return complex(float_from_json_v2(data[0]), float_from_json_v2(data[1]))


def complex_float_from_json_v3(data: tuple[JSONFloatV3, JSONFloatV3]) -> complex:
    """
    Convert a JSON complex float to a complex number (v3).

    Parameters
    ----------
    data : tuple[JSONFloat, JSONFloat]
        The JSON complex float to convert.

    Returns
    -------
    np.complexfloating
        The complex number.
    """
    return complex(float_from_json_v3(data[0]), float_from_json_v3(data[1]))


def check_json_float_v2(data: JSON) -> TypeGuard[JSONFloatV2]:
    """
    Check if a JSON value represents a float (v2).

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a float, False otherwise.
    """
    return data in ("NaN", "Infinity", "-Infinity") or isinstance(data, float | int)


def check_json_float_v3(data: JSON) -> TypeGuard[JSONFloatV3]:
    """
    Check if a JSON value represents a float (v3).

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a float, False otherwise.
    """
    return check_json_float_v2(data) or (isinstance(data, str) and data.startswith("0x"))


def check_json_complex_float_v2(data: JSON) -> TypeGuard[tuple[JSONFloatV2, JSONFloatV2]]:
    """
    Check if a JSON value represents a complex float, as per the behavior of zarr-python 2.x

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a complex float, False otherwise.
    """
    return (
        not isinstance(data, str)
        and isinstance(data, Sequence)
        and len(data) == 2
        and check_json_float_v2(data[0])
        and check_json_float_v2(data[1])
    )


def check_json_complex_float_v3(data: JSON) -> TypeGuard[tuple[JSONFloatV3, JSONFloatV3]]:
    """
    Check if a JSON value represents a complex float, as per the zarr v3 spec

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a complex float, False otherwise.
    """
    return (
        not isinstance(data, str)
        and isinstance(data, Sequence)
        and len(data) == 2
        and check_json_float_v3(data[0])
        and check_json_float_v3(data[1])
    )


def check_json_int(data: JSON) -> TypeGuard[int]:
    """
    Check if a JSON value is an integer.

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is an integer, False otherwise.
    """
    return bool(isinstance(data, int))


def check_json_intish_float(data: JSON) -> TypeGuard[IntishFloat]:
    """
    Check if a JSON value is an "intish float", i.e. a float that represents an integer, like 0.0.

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is an intish float, False otherwise.
    """
    return isinstance(data, float) and data.is_integer()


def check_json_intish_str(data: JSON) -> TypeGuard[IntishStr]:
    """
    Check if a JSON value is a string that represents an integer, like "0", "42", or "-5".

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    bool
        True if the data is a string representing an integer, False otherwise.
    """
    if not isinstance(data, str):
        return False

    try:
        int(data)
    except ValueError:
        return False
    else:
        return True


def check_json_floatish_str(data: JSON) -> TypeGuard[FloatishStr]:
    """
    Check if a JSON value is a string that represents a float, like "3.14", "-2.5", or "0.0".

    Note: This function is intended to be used AFTER check_json_float_v2/v3, so it only
    handles regular string representations that those functions don't cover.

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    bool
        True if the data is a string representing a regular float, False otherwise.
    """
    if not isinstance(data, str):
        return False

    try:
        float(data)
    except ValueError:
        return False
    else:
        return True


def check_json_str(data: JSON) -> TypeGuard[str]:
    """
    Check if a JSON value is a string.

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a string, False otherwise.
    """
    return bool(isinstance(data, str))


def check_json_bool(data: JSON) -> TypeGuard[bool]:
    """
    Check if a JSON value is a boolean.

    Parameters
    ----------
    data : JSON
        The JSON value to check.

    Returns
    -------
    Bool
        True if the data is a boolean, False otherwise.
    """
    return isinstance(data, bool)
