import sys
import os
import threading


class ChdbError(Exception):
    """Base class for exceptions in this module."""


_arrow_format = set({"dataframe", "arrowtable"})
_process_result_format_funs = {
    "dataframe": lambda x: to_df(x),
    "arrowtable": lambda x: to_arrowTable(x),
}

# If any UDF is defined, the path of the UDF will be set to this variable
# and the path will be deleted when the process exits
# UDF config path will be f"{g_udf_path}/udf_config.xml"
# UDF script path will be f"{g_udf_path}/{func_name}.py"
g_udf_path = ""

chdb_version = ('3', '3', '0')
import _chdb

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = ".".join(map(str, chdb_version))
except:  # noqa
    __version__ = "unknown"


# return pyarrow table
def to_arrowTable(res):
    """convert res to arrow table"""
    # try import pyarrow and pandas, if failed, raise ImportError with suggestion
    try:
        import pyarrow as pa  # noqa
        import pandas as pd  # noqa
    except ImportError as e:
        print(f"ImportError: {e}")
        print('Please install pyarrow and pandas via "pip install pyarrow pandas"')
        raise ImportError("Failed to import pyarrow or pandas") from None
    if len(res) == 0:
        return pa.Table.from_batches([], schema=pa.schema([]))
    return pa.RecordBatchFileReader(res.bytes()).read_all()


# return pandas dataframe
def to_df(r):
    """convert arrow table to Dataframe"""
    t = to_arrowTable(r)
    return t.to_pandas(use_threads=True)


# global connection lock, for multi-threading use of legacy chdb.query()
g_conn_lock = threading.Lock()


# wrap _chdb functions
def query(sql, output_format="CSV", path="", udf_path=""):
    global g_udf_path
    if udf_path != "":
        g_udf_path = udf_path
    conn_str = ""
    if path == "":
        conn_str = ":memory:"
    else:
        conn_str = f"{path}"
    if g_udf_path != "":
        if "?" in conn_str:
            conn_str = f"{conn_str}&udf_path={g_udf_path}"
        else:
            conn_str = f"{conn_str}?udf_path={g_udf_path}"
    if output_format == "Debug":
        output_format = "CSV"
        if "?" in conn_str:
            conn_str = f"{conn_str}&verbose&log-level=test"
        else:
            conn_str = f"{conn_str}?verbose&log-level=test"

    lower_output_format = output_format.lower()
    result_func = _process_result_format_funs.get(lower_output_format, lambda x: x)
    if lower_output_format in _arrow_format:
        output_format = "Arrow"

    with g_conn_lock:
        conn = _chdb.connect(conn_str)
        res = conn.query(sql, output_format)
        if res.has_error():
            conn.close()
            raise ChdbError(res.error_message())
        conn.close()
    return result_func(res)


# alias for query
sql = query

PyReader = _chdb.PyReader

from . import dbapi, session, udf, utils  # noqa: E402
from .state import connect  # noqa: E402

__all__ = [
    "_chdb",
    "PyReader",
    "ChdbError",
    "query",
    "sql",
    "chdb_version",
    "engine_version",
    "to_df",
    "to_arrowTable",
    "dbapi",
    "session",
    "udf",
    "utils",
    "connect",
]
