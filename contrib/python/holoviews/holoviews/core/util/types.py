import datetime as dt
import inspect
from types import GeneratorType
from typing import TYPE_CHECKING

import narwhals.stable.v2 as nw

from .dependencies import _LazyModule

if TYPE_CHECKING:
    import cftime
    import numpy as np
    import pandas as pd
else:
    cftime = _LazyModule("cftime", bool_use_sys_modules=True)
    np = _LazyModule("numpy", bool_use_sys_modules=True)
    pd = _LazyModule("pandas", bool_use_sys_modules=True)


# gen_types is copied from param, can be removed when
# we support 2.2 or greater
class _GeneratorIsMeta(type):
    def __instancecheck__(cls, inst):
        return isinstance(inst, tuple(cls.types()))

    def __subclasscheck__(cls, sub):
        return issubclass(sub, tuple(cls.types()))

    def __iter__(cls):
        yield from cls.types()


class _GeneratorIs(metaclass=_GeneratorIsMeta):
    @classmethod
    def __iter__(cls):
        yield from cls.types()


def gen_types(gen_func):
    """
    Decorator which takes a generator function which yields difference types
    make it so it can be called with isinstance and issubclass.
    """
    if not inspect.isgeneratorfunction(gen_func):
        msg = "gen_types decorator can only be applied to generator"
        raise TypeError(msg)
    return type(gen_func.__name__, (_GeneratorIs,), {"types": staticmethod(gen_func)})


# Types
generator_types = (zip, range, GeneratorType)


@gen_types
def pandas_datetime_types():
    if pd:
        from pandas.core.dtypes.dtypes import DatetimeTZDtype

        yield from (pd.Timestamp, pd.Period, DatetimeTZDtype)


@gen_types
def pandas_timedelta_types():
    if pd:
        yield pd.Timedelta


@gen_types
def cftime_types():
    if cftime:
        yield cftime.datetime


@gen_types
def datetime_types():
    yield from (dt.datetime, dt.date, dt.time)
    if np:
        yield np.datetime64
    yield from pandas_datetime_types
    yield from cftime_types


@gen_types
def timedelta_types():
    yield dt.timedelta
    if np:
        yield np.timedelta64
    yield from pandas_timedelta_types


@gen_types
def arraylike_types():
    if np:
        yield np.ndarray
    if pd:
        from pandas.core.dtypes.generic import ABCExtensionArray, ABCIndex, ABCSeries

        yield from (ABCIndex, ABCSeries, ABCExtensionArray)

    yield nw.Series

@gen_types
def masked_types():
    if np:
        yield np.ma.core.MaskedArray

    if pd:
        from pandas.core.arrays.masked import BaseMaskedArray

        yield BaseMaskedArray


__all__ = [
    "arraylike_types",
    "cftime_types",
    "datetime_types",
    "generator_types",
    "masked_types",
    "pandas_datetime_types",
    "pandas_timedelta_types",
    "timedelta_types",
]
