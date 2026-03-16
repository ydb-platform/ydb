import asyncio
import logging
import math
import re
from copy import copy
from typing import List, NamedTuple

try:
    from pytest_asyncio import fixture as asyncio_fixture
except ImportError:
    # Backward compatability for pytest-asyncio<0.17
    import pytest

    asyncio_fixture = pytest.fixture

from aiohttp import web, ClientSession
from aiohttp.client_reqrep import ClientRequest
from aiohttp.connector import TCPConnector
from aiohttp.helpers import sentinel
from aiohttp.test_utils import BaseTestServer
from aiohttp.web_request import BaseRequest
from aiohttp.web_response import StreamResponse, json_response
from aiohttp.web_runner import ServerRunner
from aiohttp.web_server import Server

from aresponses.errors import (
    NoRouteFoundError,
    UnusedRouteError,
    UnorderedRouteCallError,
)
from aresponses.utils import _text_matches_pattern, ANY

logger = logging.getLogger(__name__)


class RawResponse(StreamResponse):
    """
    Allow complete control over the response

    Useful for mocking invalid responses
    """

    def __init__(self, body):
        super().__init__()
        self._body = body

    async def _start(self, request, *_, **__):
        self._req = request
        self._keep_alive = False
        writer = self._payload_writer = request._payload_writer
        return writer

    async def write_eof(self, *_, **__):  # noqa
        await super().write_eof(self._body)


class Route:
    def __init__(
        self,
        method_pattern=ANY,
        host_pattern=ANY,
        path_pattern=ANY,
        body_pattern=ANY,
        match_querystring=False,
        repeat=1,
    ):
        self.method_pattern = method_pattern
        self.host_pattern = host_pattern
        self.path_pattern = path_pattern
        self.body_pattern = body_pattern
        self.match_querystring = match_querystring
        self.repeat = repeat

    async def matches(self, request):
        path_to_match = request.path_qs if self.match_querystring else request.path

        if not _text_matches_pattern(self.host_pattern, request.host):
            return False

        if not _text_matches_pattern(self.path_pattern, path_to_match):
            return False

        if not _text_matches_pattern(self.method_pattern, request.method.lower()):
            return False

        if self.body_pattern != ANY:
            if not _text_matches_pattern(self.body_pattern, await request.text()):
                return False

        return True

    def __str__(self):
        return (
            f"method={self.method_pattern} host_pattern={self.host_pattern} "
            f"path={self.path_pattern} body={self.body_pattern} "
            f"match_querystring={self.match_querystring}"
        )

    def __repr__(self):
        return (
            f"Route(method={repr(self.method_pattern)}, "
            f"host_pattern={repr(self.host_pattern)}, "
            f"path={repr(self.path_pattern)}, "
            f"body={repr(self.body_pattern)}, "
            f"match_querystring={repr(self.match_querystring)})"
        )


class RoutingLog(NamedTuple):
    request: BaseRequest
    route: Route
    response: StreamResponse


class ResponsesMockServer(BaseTestServer):
    ANY = ANY
    Response = web.Response
    RawResponse = RawResponse
    INFINITY = math.inf
    LOCALHOST = re.compile(r"127\.0\.0\.1:?\d{0,5}")

    def __init__(self, *, scheme=sentinel, host="127.0.0.1", **kwargs):
        self._responses = []
        self._exception = None
        self._unmatched_requests = []
        self._first_unordered_route = None
        self._request_count = 0
        self._history = []
        super().__init__(scheme=scheme, host=host, **kwargs)

    async def _make_runner(self, debug=True, **kwargs):
        srv = Server(self._handler, loop=self._loop, debug=True, **kwargs)
        return ServerRunner(srv, debug=debug, **kwargs)

    async def _handler(self, request):
        self._request_count += 1
        route, response = await self._find_response(request)
        # ensures the request content is loaded even if the handler didn't
        # need it. This makes it available in`aresponses.history`
        await request.read()
        self._history.append(RoutingLog(request, route, response))
        return response

    def add(
        self,
        host_pattern=ANY,
        path_pattern=ANY,
        method_pattern=ANY,
        response="",
        *,
        route=None,
        body_pattern=ANY,
        match_querystring=False,
        repeat=1,
    ):
        """
        Adds a route and response to the mock server.

        When the route is hit `repeat` times it will be removed from the routing table.Z

        :param host_pattern:
        :param path_pattern:
        :param method_pattern:
        :param response:
        :param route: A Route object.  Overrides all args except for `response`.
                        Useful for custom matching.
        :param body_pattern:
        :param match_querystring:
        :param repeat:
        :return:
        """
        if isinstance(host_pattern, str):
            host_pattern = host_pattern.lower()

        if isinstance(method_pattern, str):
            method_pattern = method_pattern.lower()

        if route is None:
            route = Route(
                method_pattern=method_pattern,
                host_pattern=host_pattern,
                path_pattern=path_pattern,
                body_pattern=body_pattern,
                match_querystring=match_querystring,
                repeat=repeat,
            )

        self._responses.append((route, response))

    def add_local_passthrough(self, repeat=INFINITY):
        self.add(host_pattern=self.LOCALHOST, repeat=repeat, response=self.passthrough)

    async def _find_response(self, request):
        for i, (route, response) in enumerate(self._responses):
            if not await route.matches(request):
                continue

            route.repeat -= 1

            if route.repeat <= 0:
                del self._responses[i]
            else:
                self._responses[i] = (route, copy(response))

            if callable(response):
                if asyncio.iscoroutinefunction(response):
                    response = await response(request)
                else:
                    response = response(request)

            elif isinstance(response, str):
                response = self.Response(body=response)
            elif isinstance(response, (dict, list)):
                response = json_response(data=response)

            if i > 0 and self._first_unordered_route is None:
                self._first_unordered_route = route

            return route, response

        self._unmatched_requests.append(request)
        return None, None

    async def passthrough(self, request):
        """Make non-mocked network request"""

        class DirectTcpConnector(TCPConnector):
            def _resolve_host(slf, *args, **kwargs):  # noqa
                return self._old_resolver_mock(slf, *args, **kwargs)

        class DirectClientRequest(ClientRequest):
            def is_ssl(slf) -> bool:  # noqa
                return slf._aresponses_direct_is_ssl()

        connector = DirectTcpConnector()

        original_request = request.clone(
            scheme="https" if request.headers["AResponsesIsSSL"] else "http"
        )

        headers = {k: v for k, v in request.headers.items() if k != "AResponsesIsSSL"}

        async with ClientSession(
            connector=connector, request_class=DirectClientRequest
        ) as session:
            request_method = getattr(session, request.method.lower())
            async with request_method(
                original_request.url, headers=headers, data=(await request.read())
            ) as r:
                headers = {
                    k: v for k, v in r.headers.items() if k.lower() == "content-type"
                }
                data = await r.read()
                response = self.Response(body=data, status=r.status, headers=headers)
                return response

    async def __aenter__(self) -> "ResponsesMockServer":
        await self.start_server(loop=self._loop)

        self._old_resolver_mock = TCPConnector._resolve_host

        async def _resolver_mock(_self, host, port, traces=None):
            return [
                {
                    "hostname": host,
                    "host": "127.0.0.1",
                    "port": self.port,
                    "family": _self._family,
                    "proto": 0,
                    "flags": 0,
                }
            ]

        TCPConnector._resolve_host = _resolver_mock

        self._old_is_ssl = ClientRequest.is_ssl
        ClientRequest._aresponses_direct_is_ssl = ClientRequest.is_ssl

        def new_is_ssl(_self):
            return False

        ClientRequest.is_ssl = new_is_ssl

        # store whether a request was an SSL request in the `AResponsesIsSSL` header
        self._old_init = ClientRequest.__init__

        def new_init(_self, *largs, **kwargs):
            self._old_init(_self, *largs, **kwargs)

            is_ssl = "1" if self._old_is_ssl(_self) else ""
            _self.update_headers({**_self.headers, "AResponsesIsSSL": is_ssl})

        ClientRequest.__init__ = new_init

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        TCPConnector._resolve_host = self._old_resolver_mock
        ClientRequest.is_ssl = self._old_is_ssl
        ClientRequest.__init__ = self._old_init

        await self.close()

    def assert_no_unused_routes(self, ignore_infinite_repeats=False):
        for route, _ in self._responses:
            if not ignore_infinite_repeats or route.repeat != self.INFINITY:
                raise UnusedRouteError(f"Unused Route: {route}")

    def assert_called_in_order(self):
        if self._first_unordered_route is not None:
            raise UnorderedRouteCallError(
                f"Route used out of order: {self._first_unordered_route}"
            )

    def assert_all_requests_matched(self):
        if self._unmatched_requests:
            request = self._unmatched_requests[0]
            raise NoRouteFoundError(
                f"No match found for request: "
                f"{request.method} {request.host} {request.path}"
            )

    def assert_plan_strictly_followed(self):
        self.assert_no_unused_routes()
        self.assert_called_in_order()
        self.assert_all_requests_matched()

    @property
    def history(self) -> List[RoutingLog]:
        return self._history


@asyncio_fixture()
async def aresponses() -> ResponsesMockServer:
    loop = asyncio.get_running_loop()
    async with ResponsesMockServer(loop=loop) as server:
        yield server
