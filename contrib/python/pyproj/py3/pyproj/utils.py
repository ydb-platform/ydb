"""
Utility functions used within pyproj
"""

import json
from array import array
from enum import Enum, auto
from typing import Any


def is_null(value: Any) -> bool:
    """
    Check if value is NaN or None
    """
    # pylint: disable=comparison-with-itself
    return value != value or value is None


def strtobool(value: Any) -> bool:
    """
    https://docs.python.org/3.9/distutils/apiref.html#distutils.util.strtobool

    Here since distutils is deprecated.

    Convert a string representation of truth to True or False.
    """
    value = str(value).lower()
    if value in ("y", "yes", "t", "true", "on", "1"):
        return True
    if value in ("n", "no", "f", "false", "off", "0"):
        return False
    raise ValueError(f"invalid truth value: '{value}'")


class NumpyEncoder(json.JSONEncoder):
    """
    Handle numpy types when dumping to JSON
    """

    def default(self, obj):  # pylint: disable=arguments-renamed
        try:
            return obj.tolist()
        except AttributeError:
            pass
        try:
            # numpy scalars
            if obj.dtype.kind == "f":
                return float(obj)
            if obj.dtype.kind == "i":
                return int(obj)
        except AttributeError:
            pass
        return json.JSONEncoder.default(self, obj)


class DataType(Enum):
    """
    Data type for copy to buffer and convertback operations
    """

    FLOAT = auto()
    LIST = auto()
    TUPLE = auto()
    ARRAY = auto()


def _copytobuffer_return_scalar(xxx: Any) -> tuple[array, DataType]:
    """
    Prepares scalar for PROJ C-API:
    - Makes a copy because PROJ modifies buffer in place
    - Make sure dtype is double as that is what PROJ expects
    - Makes sure object supports Python Buffer API

    Parameters
    -----------
    xxx: float or 0-d numpy array

    Returns
    -------
    tuple[Any, DataType]
        The copy of the data prepared for the PROJ API & Python Buffer API.
    """
    try:
        return array("d", (float(xxx),)), DataType.FLOAT
    except Exception:
        raise TypeError("input must be a scalar") from None


def _copytobuffer(xxx: Any, inplace: bool = False) -> tuple[Any, DataType]:
    """
    Prepares data for PROJ C-API:
    - Makes a copy because PROJ modifies buffer in place
    - Make sure dtype is double as that is what PROJ expects
    - Makes sure object supports Python Buffer API

    If the data is a numpy array, it ensures the data is in C order.

    Parameters
    ----------
    xxx: Any
        A scalar, list, tuple, numpy.array,
        pandas.Series, xaray.DataArray, or dask.array.Array.
    inplace: bool, default=False
        If True, will return the array without a copy if it
        meets the requirements of the Python Buffer API & PROJ C-API.

    Returns
    -------
    tuple[Any, DataType]
        The copy of the data prepared for the PROJ API & Python Buffer API.
    """
    # check for pandas.Series, xarray.DataArray or dask.array.Array
    # also handle numpy masked Arrays; note that pandas.Series also has a
    # "mask" attribute, hence checking for simply the "mask" attr in that
    # case isn't sufficient
    if (
        not hasattr(xxx, "hardmask")
        and hasattr(xxx, "__array__")
        and callable(xxx.__array__)
    ):
        xxx = xxx.__array__()

    # handle numpy data
    if hasattr(xxx, "shape"):
        if xxx.shape == ():
            # convert numpy array scalar to float
            # (array scalars don't support buffer API)
            return _copytobuffer_return_scalar(xxx)
        # Use C order when copying to handle arrays in fortran order
        return xxx.astype("d", order="C", copy=not inplace), DataType.ARRAY
    data_type = DataType.ARRAY
    if isinstance(xxx, array):
        if not inplace or xxx.typecode != "d":
            xxx = array("d", xxx)
    elif isinstance(xxx, list):
        xxx = array("d", xxx)
        data_type = DataType.LIST
    elif isinstance(xxx, tuple):
        xxx = array("d", xxx)
        data_type = DataType.TUPLE
    else:
        return _copytobuffer_return_scalar(xxx)
    return xxx, data_type


def _convertback(data_type: DataType, inx: Any) -> Any:
    # if inputs were lists, tuples or floats, convert back to original type.
    if data_type == DataType.FLOAT:
        return inx[0]
    if data_type == DataType.LIST:
        return inx.tolist()
    if data_type == DataType.TUPLE:
        return tuple(inx)
    return inx
