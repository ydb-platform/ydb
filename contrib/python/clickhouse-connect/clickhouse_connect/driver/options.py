import warnings

from clickhouse_connect.driver.exceptions import NotSupportedError

# Attributes resolved lazily by __getattr__ / _resolve_* functions:
#   np, pd, arrow, pl, pd_time_test, pd_extended_dtypes, PANDAS_VERSION, IS_PANDAS_2

# pylint: disable=import-outside-toplevel

_PANDAS_ATTRS = frozenset({"pd", "pd_time_test", "pd_extended_dtypes", "PANDAS_VERSION", "IS_PANDAS_2"})
_ALL_LAZY = frozenset({"np", "arrow", "pl"}) | _PANDAS_ATTRS


def _resolve_numpy():
    if "np" in globals():
        return
    try:
        import numpy

        globals()["np"] = numpy
    except ImportError:
        globals()["np"] = None


def _resolve_pandas():
    if "pd" in globals():
        return
    try:
        import pandas

        globals()["pd"] = pandas
        version = tuple(map(int, pandas.__version__.split(".")[:2]))
        globals()["PANDAS_VERSION"] = version
        is_v2 = version >= (2, 0)
        globals()["IS_PANDAS_2"] = is_v2
        globals()["pd_extended_dtypes"] = not pandas.__version__.startswith("0")
        if not is_v2:
            warnings.warn(
                "clickhouse-connect support for pandas 1.x is deprecated and will be removed in v1.0.0. "
                "Please upgrade to pandas 2.x or later.",
                DeprecationWarning,
                stacklevel=2,
            )
        try:
            from pandas.core.dtypes.common import (
                is_datetime64_dtype,
                is_timedelta64_dtype,
            )

            def combined_test(arr_or_dtype):
                return is_datetime64_dtype(arr_or_dtype) or is_timedelta64_dtype(arr_or_dtype)

            globals()["pd_time_test"] = combined_test
        except ImportError:
            try:
                from pandas.core.dtypes.common import is_datetime_or_timedelta_dtype

                globals()["pd_time_test"] = is_datetime_or_timedelta_dtype
            except ImportError as ex:
                raise NotSupportedError("pandas version does not contain expected test for temporal types") from ex
    except ImportError:
        globals()["pd"] = None
        globals()["PANDAS_VERSION"] = None
        globals()["IS_PANDAS_2"] = None
        globals()["pd_extended_dtypes"] = False
        globals()["pd_time_test"] = None


def _resolve_arrow():
    if "arrow" in globals():
        return
    try:
        import pyarrow

        globals()["arrow"] = pyarrow
    except ImportError:
        globals()["arrow"] = None


def _resolve_polars():
    if "pl" in globals():
        return
    try:
        import polars

        globals()["pl"] = polars
    except ImportError:
        globals()["pl"] = None


def __getattr__(name):
    if name in _PANDAS_ATTRS:
        _resolve_pandas()
    elif name == "np":
        _resolve_numpy()
    elif name == "arrow":
        _resolve_arrow()
    elif name == "pl":
        _resolve_polars()
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return globals()[name]


def __dir__():
    return list(globals().keys()) + list(_ALL_LAZY - globals().keys())


# pylint: disable=redefined-outer-name
def check_numpy():
    _resolve_numpy()
    np = globals()["np"]
    if np:
        return np
    raise NotSupportedError("Numpy package is not installed")


def check_pandas():
    _resolve_pandas()
    pd = globals()["pd"]
    if pd:
        return pd
    raise NotSupportedError("Pandas package is not installed")


def check_arrow():
    _resolve_arrow()
    arrow = globals()["arrow"]
    if arrow:
        return arrow
    raise NotSupportedError("PyArrow package is not installed")


def check_polars():
    _resolve_polars()
    pl = globals()["pl"]
    if pl:
        return pl
    raise NotSupportedError("Polars package is not installed")
