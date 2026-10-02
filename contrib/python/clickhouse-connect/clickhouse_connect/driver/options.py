from clickhouse_connect.driver.exceptions import NotSupportedError

# Attributes resolved lazily by __getattr__ / _resolve_* functions:
#   np, pd, arrow, pl, pd_time_test


_PANDAS_ATTRS = frozenset({"pd", "pd_time_test"})
_ALL_LAZY = frozenset({"np", "arrow", "pl"}) | _PANDAS_ATTRS


def _pd_time_test(arr_or_dtype):
    """Check whether a Series or dtype is datetime64 or timedelta64."""
    kind = getattr(arr_or_dtype, "kind", None)
    if kind is None:
        kind = getattr(getattr(arr_or_dtype, "dtype", None), "kind", None)
    return kind in ("M", "m")


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

        version = tuple(map(int, pandas.__version__.split(".")[:2]))
        if version < (2, 0):
            raise NotSupportedError(
                f"clickhouse-connect requires pandas 2.0 or later, found {pandas.__version__}. Please upgrade: pip install --upgrade pandas"
            )
        globals()["pd"] = pandas
        globals()["pd_time_test"] = _pd_time_test
    except ImportError:
        globals()["pd"] = None
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
