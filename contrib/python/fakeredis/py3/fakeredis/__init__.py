import sys

from ._connection import (
    FakeRedis,
    FakeStrictRedis,
    FakeConnection,
)
from ._server import FakeServer
from ._valkey import FakeValkey, FakeAsyncValkey, FakeStrictValkey
from .aioredis import (
    FakeRedis as FakeAsyncRedis,
    FakeConnection as FakeAsyncConnection,
)

if sys.version_info >= (3, 11):
    from ._tcp_server import TcpFakeServer
else:

    class TcpFakeServer:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("TcpFakeServer is only available in Python 3.11+")


try:
    from importlib import metadata
except ImportError:  # for Python < 3.8
    import importlib_metadata as metadata  # type: ignore
__version__ = metadata.version("fakeredis")
__author__ = "Daniel Moran"
__maintainer__ = "Daniel Moran"
__email__ = "daniel@moransoftware.ca"
__license__ = "BSD-3-Clause"
__url__ = "https://github.com/cunla/fakeredis-py"
__bugtrack_url__ = "https://github.com/cunla/fakeredis-py/issues"

__all__ = [
    "FakeServer",
    "FakeRedis",
    "FakeStrictRedis",
    "FakeConnection",
    "FakeAsyncRedis",
    "FakeAsyncConnection",
    "TcpFakeServer",
    "FakeValkey",
    "FakeAsyncValkey",
    "FakeStrictValkey",
]
