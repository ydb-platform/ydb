import dataclasses
import pathlib
import socket
import typing

import aiohttp.web

from testsuite import types
from testsuite.utils import callinfo, http, url_util

GenericRequestHandler = typing.Callable[
    ...,
    types.MaybeAsyncResult[aiohttp.web.Response],
]
GenericRequestDecorator = typing.Callable[
    [GenericRequestHandler],
    callinfo.AsyncCallQueue,
]
JsonRequestHandler = typing.Callable[
    ...,
    types.MaybeAsyncResult[
        typing.Union[aiohttp.web.Response, types.JsonAnyOptional]
    ],
]
JsonRequestDecorator = typing.Callable[
    [JsonRequestHandler],
    callinfo.AsyncCallQueue,
]
MockserverRequest = http.Request


@dataclasses.dataclass(frozen=True)
class SslCertInfo:
    cert_path: str
    private_key_path: str


@dataclasses.dataclass(frozen=True)
class MockserverInfo:
    host: str
    port: int
    base_url: str
    socket_path: pathlib.Path | None = None
    https: bool = False

    def url(self, path: str) -> str:
        """Concats ``base_url`` and provided ``path``."""
        return url_util.join(self.base_url, path)

    def get_host_header(self) -> str:
        if self.socket_path:
            return str(self.socket_path)

        if not self.host and not self.port:
            raise RuntimeError(
                'either host and port or socket_path must be set in mockserver info'
            )

        if self.port == 80:
            return str(self.host)
        return f'{self.host}:{self.port}'

    def ws_url(self, path: str) -> str:
        schema = 'wss' if self.https else 'ws'
        return url_util.join(f'{schema}://{self.host}:{self.port}/', path)


@dataclasses.dataclass(frozen=True)
class MockserverSocket:
    info: MockserverInfo
    sockets: list[socket.socket]


@dataclasses.dataclass(frozen=True)
class MockserverConfig:
    nofail: bool = False
    debug: bool = False
    tracing_enabled: bool = False
    trace_id_header: str = ''
    span_id_header: str = ''
    http_proxy_enabled: bool = False


MockserverInfoFixture = MockserverInfo
MockserverSslInfoFixture = typing.Optional[MockserverInfo]
