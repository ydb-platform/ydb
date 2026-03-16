from __future__ import annotations

try:
    import socks  # type: ignore[import]
except ImportError:
    import warnings

    from urllib3.exceptions import DependencyWarning

    warnings.warn(
        (
            "SOCKS support in urllib3 requires the installation of optional "
            "dependencies: specifically, PySocks.  For more information, see "
            "https://urllib3.readthedocs.io/en/latest/contrib.html#socks-proxies"
        ),
        DependencyWarning,
    )
    raise

import typing
from urllib3.contrib.socks import SOCKSProxyManager, SOCKSConnection

from .connection import RawHTTPConnection, RawHTTPSConnection
from .connectionpool import RawHTTPConnectionPool, RawHTTPSConnectionPool


try:
    from typing import TypedDict

    class _TYPE_SOCKS_OPTIONS(TypedDict):
        socks_version: int
        proxy_host: str | None
        proxy_port: str | None
        username: str | None
        password: str | None
        rdns: bool

except ImportError:  # Python 3.7
    _TYPE_SOCKS_OPTIONS = typing.Dict[str, typing.Any]  # type: ignore[misc, assignment]


class RawSOCKSConnection(SOCKSConnection, RawHTTPConnection):
    pass


class RawSOCKSHTTPSConnection(RawSOCKSConnection, RawHTTPSConnection):
    pass


class RawSOCKSHTTPConnectionPool(RawHTTPConnectionPool):
    ConnectionCls = RawSOCKSConnection


class RawSOCKSHTTPSConnectionPool(RawHTTPSConnectionPool):
    ConnectionCls = RawSOCKSHTTPSConnection


class RawSOCKSProxyManager(SOCKSProxyManager):
    pool_classes_by_scheme = {
        "http": RawSOCKSHTTPConnectionPool,
        "https": RawSOCKSHTTPSConnectionPool,
    }

    def __init__(
            self,
            proxy_url: str,
            username: str | None = None,
            password: str | None = None,
            num_pools: int = 10,
            headers: typing.Mapping[str, str] | None = None,
            **connection_pool_kw: typing.Any,
    ):
        super().__init__(
            proxy_url,
            username=username, password=password,
            num_pools=num_pools, headers=headers, **connection_pool_kw
        )

        self.pool_classes_by_scheme = RawSOCKSProxyManager.pool_classes_by_scheme
