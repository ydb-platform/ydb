"""DuckDB function constants and types. DEPRECATED: please use `duckdb.func` instead."""

import warnings

from duckdb.func import ARROW, DEFAULT, NATIVE, SPECIAL, FunctionNullHandling, PythonUDFType

__all__ = ["ARROW", "DEFAULT", "NATIVE", "SPECIAL", "FunctionNullHandling", "PythonUDFType"]

warnings.warn(
    "`duckdb.functional` is deprecated and will be removed in a future version. Please use `duckdb.func` instead.",
    DeprecationWarning,
    stacklevel=2,
)
