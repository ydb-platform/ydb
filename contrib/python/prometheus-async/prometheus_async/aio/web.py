# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
aiohttp-based metrics exposure.
"""

from __future__ import annotations

import asyncio
import queue
import threading

from typing import TYPE_CHECKING, NamedTuple

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from prometheus_client.openmetrics import exposition as openmetrics


if TYPE_CHECKING:
    import ssl

    from typing import Callable

    from ..types import Deregisterer, ServiceDiscovery


def _choose_generator(accept_header: str | None) -> tuple[Callable, str]:
    """
    Return the correct generate function according to *accept_header*.

    Default to the old style.
    """
    accept_header = accept_header or ""
    for accepted in accept_header.split(","):
        if accepted.split(";")[0].strip() == "application/openmetrics-text":
            return (
                openmetrics.generate_latest,
                openmetrics.CONTENT_TYPE_LATEST,
            )

    return generate_latest, CONTENT_TYPE_LATEST


async def server_stats(request: web.Request) -> web.Response:
    """
    Return a web response with the plain text version of the metrics.

    :rtype: :class:`aiohttp.web.Response`
    """
    generate, content_type = _choose_generator(request.headers.get("Accept"))

    rsp = web.Response(body=generate(REGISTRY))
    # This is set separately because aiohttp complains about `;` in
    # content_type thinking it means there's also a charset.
    # cf. https://github.com/aio-libs/aiohttp/issues/2197
    rsp.content_type = content_type

    return rsp


_REF = '<html><body><a href="/metrics">Metrics</a></body></html>'


async def _cheap(request: web.Request) -> web.Response:
    """
    A view that links to metrics.

    Useful for cheap health checks.
    """
    return web.Response(text=_REF, content_type="text/html")


async def start_http_server(
    *,
    addr: str = "",
    port: int = 0,
    ssl_ctx: ssl.SSLContext | None = None,
    service_discovery: ServiceDiscovery | None = None,
) -> MetricsHTTPServer:
    """
    Start an HTTP(S) server on *addr*:*port*.

    If *ssl_ctx* is set, use TLS.

    :param str addr: Interface to listen on. Leaving empty will listen on all
        interfaces.
    :param int port: Port to listen on.
    :param ssl.SSLContext ssl_ctx: TLS settings
    :param service_discovery: see :ref:`sd`

    :rtype: MetricsHTTPServer

    .. deprecated:: 18.2.0

       The *loop* argument is a no-op now and will be removed in one year by
       the earliest.
    .. versionchanged:: 21.1.0 The *loop* argument has been removed.
    """
    app = web.Application()
    app.router.add_get("/", _cheap)
    app.router.add_get("/metrics", server_stats)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, addr, port, ssl_context=ssl_ctx)
    await site.start()

    ms = MetricsHTTPServer.from_server(
        runner=runner, app=app, https=ssl_ctx is not None
    )
    if service_discovery is not None:
        ms._deregister = await service_discovery.register(ms)

    return ms


class MetricsHTTPServer:
    """
    A stoppable metrics HTTP server.

    Returned by :func:`start_http_server`.  Do *not* instantiate it yourself.

    :ivar socket: Socket the server is listening on.  namedtuple of
        either (:class:`ipaddress.IPv4Address`, port) or
        (:class:`ipaddress.IPv6Address`, port).
    :ivar bool https: Whether the server uses SSL/TLS.
    :ivar str url: A valid URL to the metrics endpoint.
    :ivar bool is_registered: Is the web endpoint registered with a
        service discovery system?
    """

    socket: Socket
    https: bool
    _runner: web.AppRunner
    _app: web.Application
    _deregister: Deregisterer | None

    def __init__(
        self,
        socket: Socket,
        runner: web.AppRunner,
        app: web.Application,
        https: bool,
    ):
        self._app = app
        self._runner = runner
        self._deregister = None

        self.socket = socket
        self.https = https

    @classmethod
    def from_server(
        cls, runner: web.AppRunner, app: web.Application, https: bool
    ) -> MetricsHTTPServer:
        return cls(
            socket=Socket(*runner.addresses[0][:2]),
            runner=runner,
            app=app,
            https=https,
        )

    @property
    def is_registered(self) -> bool:
        """
        Is the web endpoint registered with a service discovery system?
        """
        return self._deregister is not None

    @property
    def url(self) -> str:
        addr = self.socket.addr
        return "http{s}://{host}:{port}/".format(
            s="s" if self.https else "",
            host=addr if ":" not in addr else f"[{addr}]",
            port=self.socket.port,
        )

    async def close(self) -> None:
        """
        Stop the server and clean up.
        """
        if self._deregister is not None:
            await self._deregister()
        await self._runner.cleanup()


class Socket(NamedTuple):
    addr: str
    port: int


class ThreadedMetricsHTTPServer:
    """
    A stoppable metrics HTTP server that runs in a separate thread.

    Returned by :func:`start_http_server_in_thread`.  Do *not* instantiate it
    yourself.

    :ivar socket: Socket the server is listening on.  namedtuple of
        ``Socket(addr, port)``.
    :ivar bool https: Whether the server uses SSL/TLS.
    :ivar str url: A valid URL to the metrics endpoint.
    :ivar bool is_registered: Is the web endpoint registered with a
        service discovery system?
    """

    def __init__(
        self,
        http_server: MetricsHTTPServer,
        thread: threading.Thread,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._http_server = http_server
        self._thread = thread
        self._loop = loop

    def close(self) -> None:
        """
        Stop the server, close the event loop, and join the thread.
        """
        self._loop.call_soon_threadsafe(self._loop.stop)

        self._thread.join()
        self._loop.close()

    @property
    def https(self) -> bool:
        return self._http_server.https

    @property
    def socket(self) -> Socket:
        return self._http_server.socket

    @property
    def url(self) -> str:
        return self._http_server.url

    @property
    def is_registered(self) -> bool:
        return self._http_server.is_registered


def start_http_server_in_thread(
    *,
    port: int = 0,
    addr: str = "",
    ssl_ctx: ssl.SSLContext | None = None,
    service_discovery: ServiceDiscovery | None = None,
) -> ThreadedMetricsHTTPServer:
    """
    Start an asyncio HTTP(S) server in a new thread with an own event loop.

    Ideal to expose your metrics in non-asyncio Python 3 applications.

    For arguments see :func:`start_http_server`.

    :rtype: ThreadedMetricsHTTPServer
    """
    q: queue.Queue = queue.Queue()
    loop = asyncio.new_event_loop()

    def server() -> None:
        asyncio.set_event_loop(loop)
        http = loop.run_until_complete(
            start_http_server(
                port=port,
                addr=addr,
                ssl_ctx=ssl_ctx,
                service_discovery=service_discovery,
            )
        )
        q.put(http)
        loop.run_forever()
        loop.run_until_complete(http.close())

    t = threading.Thread(
        target=server, name="PrometheusAsyncWebEndpoint", daemon=True
    )
    t.start()

    return ThreadedMetricsHTTPServer(q.get(), t, loop)
