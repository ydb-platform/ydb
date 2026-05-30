import sys

if sys.version_info < (3, 10):  # noqa: UP036
    raise RuntimeError("clickhouse-connect 1.0+ requires Python 3.10 or later. Python 3.9 users should pin to clickhouse-connect<1.0.")

from clickhouse_connect._version import version as __version__
from clickhouse_connect.driver import create_async_client, create_client

__all__ = ["__version__", "driver_name", "get_client", "get_async_client"]

driver_name = "clickhousedb"

get_client = create_client
get_async_client = create_async_client
