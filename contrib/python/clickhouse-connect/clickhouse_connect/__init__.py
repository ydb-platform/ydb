import sys
import warnings

from clickhouse_connect.driver import create_client, create_async_client


if sys.version_info < (3, 10):
    warnings.warn(
        "Python 3.9 support is deprecated and will be removed in a future release. "
        "This version of clickhouse-connect may stop working with Python 3.9 unexpectedly.",
        DeprecationWarning,
        stacklevel=2
    )


driver_name = 'clickhousedb'

get_client = create_client
get_async_client = create_async_client
