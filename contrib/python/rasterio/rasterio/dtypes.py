"""Mapping of GDAL to Numpy data types."""

import numpy

from rasterio.env import _GDAL_AT_LEAST_3_11

bool_ = numpy.dtype(bool).name
ubyte = uint8 = numpy.uint8().dtype.name
sbyte = int8 = numpy.int8().dtype.name
uint16 = numpy.uint16().dtype.name
int16 = numpy.int16().dtype.name
uint32 = numpy.uint32().dtype.name
int32 = numpy.int32().dtype.name
uint64 = numpy.uint64().dtype.name
int64 = numpy.int64().dtype.name
float16 = numpy.float16().dtype.name
float32 = numpy.float32().dtype.name
float64 = numpy.float64().dtype.name
complex_ = "complex"  # alias for python built-in complex which maps to numpy.complex128
complex64 = numpy.complex64().dtype.name
complex128 = numpy.complex128().dtype.name
complex_int16 = "complex_int16"

dtype_fwd = {
    0: None,  # GDT_Unknown
    1: ubyte,  # GDT_Byte
    2: uint16,  # GDT_UInt16
    3: int16,  # GDT_Int16
    4: uint32,  # GDT_UInt32
    5: int32,  # GDT_Int32
    6: float32,  # GDT_Float32
    7: float64,  # GDT_Float64
    8: complex_int16,  # GDT_CInt16
    9: complex64,  # GDT_CInt32
    10: complex64,  # GDT_CFloat32
    11: complex128,  # GDT_CFloat64
    12: uint64,  # GDT_UInt64
    13: int64, # GDT_Int64
    14: sbyte,  # GDT_Int8
}

if _GDAL_AT_LEAST_3_11:
    dtype_fwd[15] = float16  # GDT_Float16
    dtype_fwd[16] = complex64  # GDT_CFloat16

dtype_rev = {v: k for k, v in dtype_fwd.items()}

dtype_rev[uint8] = 1
dtype_rev[complex_] = 11
dtype_rev[complex64] = 10
dtype_rev[complex_int16] = 8


def _get_gdal_dtype(type_name):
    try:
        return dtype_rev[type_name]
    except KeyError:
        raise TypeError(
            f"Unsupported data type {type_name}. "
            f"Allowed data types: {list(dtype_rev)}."
        )

typename_fwd = {
    0: "Unknown",
    1: "Byte",
    2: "UInt16",
    3: "Int16",
    4: "UInt32",
    5: "Int32",
    6: "Float32",
    7: "Float64",
    8: "CInt16",
    9: "CInt32",
    10: "CFloat32",
    11: "CFloat64",
    12: "UInt64",
    13: "Int64",
    14: "Int8",
}

if _GDAL_AT_LEAST_3_11:
    typename_fwd[15] = "Float16"
    typename_fwd[16] = "CFloat16"

typename_rev = {v: k for k, v in typename_fwd.items()}

f16i = numpy.finfo(numpy.float16)
f32i = numpy.finfo(numpy.float32)
f64i = numpy.finfo(numpy.float64)

dtype_ranges = {
    int8: (-128, 127),
    uint8: (0, 255),
    uint16: (0, 65535),
    int16: (-32768, 32767),
    uint32: (0, 4294967295),
    int32: (-2147483648, 2147483647),
    float16: (float(f16i.min), float(f16i.max)),
    float32: (float(f32i.min), float(f32i.max)),
    float64: (float(f64i.min), float(f64i.max)),
    int64: (-9223372036854775808, 9223372036854775807),
    uint64: (0, 18446744073709551615),
}

dtype_info_registry = {"c": numpy.finfo, "f": numpy.finfo, "i": numpy.iinfo, "u": numpy.iinfo}


def in_dtype_range(value, dtype):
    """Test if the value is within the dtype's range of values, Nan, or Inf."""
    # The name of this function is a misnomer. What we're actually
    # testing is whether the value can be represented by the data type.
    kind = numpy.dtype(dtype).kind

    # Nan and infinity are special cases.
    if kind == "f" and (numpy.isnan(value) or numpy.isinf(value)):
        return True

    info = dtype_info_registry[kind](dtype)
    return info.min <= value <= info.max


def _gdal_typename(dt):
    try:
        return typename_fwd[dtype_rev[dt]]
    except KeyError:
        return typename_fwd[dtype_rev[dt().dtype.name]]


def check_dtype(dt):
    """Check if dtype is a known dtype."""
    if str(dt) in dtype_rev:
        return True
    elif callable(dt) and str(dt().dtype) in dtype_rev:
        return True
    return False


def get_minimum_dtype(values):
    """Determine minimum type to represent values.

    Uses range checking to determine the minimum integer or floating point
    data type required to represent values.

    Parameters
    ----------
    values: list-like


    Returns
    -------
    rasterio dtype string
    """
    values = numpy.asanyarray(values)
    min_value = values.min()
    max_value = values.max()

    if values.dtype.kind in {'i', 'u'}:
        if min_value >= 0:
            if max_value <= 255:
                return uint8
            elif max_value <= 65535:
                return uint16
            elif max_value <= 4294967295:
                return uint32
            return uint64
        elif min_value >= -128 and max_value <= 127:
            return int8
        elif min_value >= -32768 and max_value <= 32767:
            return int16
        elif min_value >= -2147483648 and max_value <= 2147483647:
            return int32
        return int64
    if _GDAL_AT_LEAST_3_11 and min_value >= f16i.min and max_value <= f16i.max:
        return float16
    if min_value >= -3.4028235e+38 and max_value <= 3.4028235e+38:
        return float32
    return float64


def is_ndarray(array):
    """Check if array is a ndarray."""

    return isinstance(array, numpy.ndarray) or hasattr(array, '__array__')


def can_cast_dtype(values, dtype):
    """Test if values can be cast to dtype without loss of information.

    Parameters
    ----------
    values: list-like
    dtype: numpy.dtype or string

    Returns
    -------
    boolean
        True if values can be cast to data type.
    """
    values = numpy.asanyarray(values)

    if values.dtype.name == _getnpdtype(dtype).name:
        return True

    elif values.dtype.kind == 'f':
        return numpy.allclose(values, values.astype(dtype), equal_nan=True)

    else:
        return numpy.array_equal(values, values.astype(dtype))


def validate_dtype(values, valid_dtypes):
    """Test if dtype of values is one of valid_dtypes.

    Parameters
    ----------
    values: list-like
    valid_dtypes: list-like
        list of valid dtype strings, e.g., ('int16', 'int32')

    Returns
    -------
    boolean:
        True if dtype of values is one of valid_dtypes
    """
    values = numpy.asanyarray(values)

    return (values.dtype.name in valid_dtypes or
            get_minimum_dtype(values) in valid_dtypes)


def _is_complex_int(dtype):
    return isinstance(dtype, str) and dtype.startswith("complex_int")


def _getnpdtype(dtype):
    if _is_complex_int(dtype):
        return numpy.complex64().dtype
    return numpy.dtype(dtype)
