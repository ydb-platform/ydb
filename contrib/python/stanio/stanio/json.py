"""
Utilities for writing Stan Json files
"""
try:
    import ujson as json

    uj_version = tuple(map(int, json.__version__.split(".")))
    if uj_version < (5, 5, 0):
        raise ImportError("ujson version too old")  # pragma: no cover
    UJSON_AVAILABLE = True
except:
    UJSON_AVAILABLE = False
    import json

from typing import Any, Mapping

import numpy as np


def process_dictionary(d: Mapping[str, Any]) -> Mapping[str, Any]:
    return {k: process_value(v) for k, v in d.items()}


# pylint: disable=too-many-return-statements
def process_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, bool):  # stan uses 0, 1
        return int(val)
    if isinstance(val, complex):  # treat as 2-long array
        return [val.real, val.imag]
    if isinstance(val, dict):  # if a tuple was manually specified
        return process_dictionary(val)
    if isinstance(val, tuple):  # otherwise, turn a tuple into a dict
        return dict(zip(range(1, len(val) + 1), map(process_value, val)))
    if isinstance(val, list):
        return [process_value(i) for i in val]
    original_module = getattr(type(val), "__module__", "")
    if (
        "numpy" in original_module
        or "xarray" in original_module
        or "pandas" in original_module
    ):
        numpy_val = np.asanyarray(val)
        # fast paths for numeric types
        if numpy_val.dtype.kind in "iuf":
            return numpy_val.tolist()
        if numpy_val.dtype.kind == "c":
            return np.stack([np.asarray(numpy_val.real), np.asarray(numpy_val.imag)], axis=-1).tolist()
        if numpy_val.dtype.kind == "b":
            return numpy_val.astype(int).tolist()

        # should only be object arrays (tuples, etc)
        return process_value(numpy_val.tolist())

    return val


def dump_stan_json(data: Mapping[str, Any]) -> str:
    """
    Convert a mapping of strings to data to a JSON string.

    Values can be any numeric type, a boolean (converted to int),
    or any collection compatible with :func:`numpy.asarray`, e.g a
    :class:`pandas.Series`.

    Produces a string compatible with the
    `Json Format for Cmdstan
    <https://mc-stan.org/docs/cmdstan-guide/json.html>`__

    :param data: A mapping from strings to values. This can be a dictionary
        or something more exotic like an :class:`xarray.Dataset`. This will be
        copied before type conversion, not modified
    """
    return json.dumps(process_dictionary(data))


def write_stan_json(path: str, data: Mapping[str, Any]) -> None:
    """
    Dump a mapping of strings to data to a JSON file.

    Values can be any numeric type, a boolean (converted to int),
    or any collection compatible with :func:`numpy.asarray`, e.g a
    :class:`pandas.Series`.

    Produces a file compatible with the
    `Json Format for Cmdstan
    <https://mc-stan.org/docs/cmdstan-guide/json.html>`__

    :param path: File path for the created json. Will be overwritten if
        already in existence.

    :param data: A mapping from strings to values. This can be a dictionary
        or something more exotic like an :class:`xarray.Dataset`. This will be
        copied before type conversion, not modified
    """
    with open(path, "w") as fd:
        if UJSON_AVAILABLE:
            json.dump(process_dictionary(data), fd)
        else:
            for chunk in json.JSONEncoder().iterencode(process_dictionary(data)):
                fd.write(chunk)
