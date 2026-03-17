import asyncio
import contextlib
import itertools
import logging
import pathlib
import re
import ssl
import time
import typing
import urllib.parse
import warnings

import aiohttp.web
import yarl

from testsuite import utils
from testsuite.tracing import TraceidManager
from testsuite.utils import (
    cached_property,
    callinfo,
    http,
    url_util,
)
from testsuite.utils import net as net_utils

from . import classes, exceptions, magicargs
from .exceptions import __tracebackhide__  # noqa: F401

DEFAULT_TRACE_ID_HEADER = 'X-YaTraceId'
DEFAULT_SPAN_ID_HEADER = 'X-YaSpanId'

REQUEST_FROM_ANOTHER_TEST_ERROR = 'Internal error: request is from other test'

_SUPPORTED_ERRORS_HEADER = 'X-Testsuite-Supported-Errors'
_ERROR_HEADER = 'X-Testsuite-Error'

_LOGGER_HEADERS = (
    ('X-YaTraceId', 'trace_id'),
    ('X-YaSpanId', 'span_id'),
    ('X-YaRequestId', 'link'),
)

logger = logging.getLogger(__name__)

RouteParams = dict[str, str]


class MockserverRequest(aiohttp.web.BaseRequest):
    # We need original path including scheme and hostname
    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.original_path = _path_from_message(message)


class Handler:
    def __init__(
        self,
        func: typing.Callable,
        *,
        raw_request: bool = False,
        json_response: bool = False,
        strict: bool = False,
    ) -> None:
        self.raw_request = raw_request
        self.json_response = json_response
        self.orig_func = func
        self.strict = strict

    @cached_property
    def callqueue(self):
        return callinfo.acallqueue(self.orig_func)

    @cached_property
    def handler_args(self):
        return magicargs.MagicArgsHandler(
            self.orig_func,
            raw_request=self.raw_request,
        )

    def __repr__(self):
        return (
            f'{Handler.__module__}.{Handler.__name__}({self.orig_func!r}, '
            f'raw_request={self.handler_args.raw_request}, '
            f'json_response={self.json_response})'
        )

    async def __call__(self, request: aiohttp.web.BaseRequest, **kwargs):
        args, kwargs = await self.handler_args.build_args(request, kwargs)
        response = await self.callqueue(*args, **kwargs)
        if not self.json_response:
            return response
        if isinstance(response, (http.Response, aiohttp.web.Response)):
            return response
        return http.make_response(json=response)

    def collect_calls(self) -> list[dict]:
        if not self.strict:
            return []

        callqueue = self.callqueue
        lost_calls = []
        while callqueue.has_calls:
            lost_calls.append(callqueue.next_call())
        return lost_calls


class Session:
    handlers: dict[str, Handler]
    prefix_handlers: list[tuple[str, Handler]]
    regex_handlers: list[tuple[typing.Pattern, Handler]]

    def __init__(
        self,
        *,
        asyncexc_append,
        traceid_manager: TraceidManager,
        tracing_enabled=True,
        http_proxy_enabled=False,
        mockserver_host=None,
    ):
        self.traceid_manager = traceid_manager
        self.tracing_enabled = tracing_enabled
        self.handlers = {}
        self.prefix_handlers = []
        self.regex_handlers = []
        self.http_proxy_enabled = http_proxy_enabled
        self.mockserver_host = mockserver_host
        self._asyncexc_append = asyncexc_append

    def get_handler(self, path: str) -> tuple[Handler, RouteParams]:
        handler = self.handlers.get(path)
        if handler is not None:
            return handler, {}
        for pattern, handler in reversed(self.regex_handlers):
            match = pattern.fullmatch(path)
            if match:
                return handler, match.groupdict()
        for prefix, handler in reversed(self.prefix_handlers):
            if path.startswith(prefix):
                return handler, {}
        raise exceptions.HandlerNotFoundError(
            self._get_handler_not_found_message(path),
        )

    def _get_handler_not_found_message(self, path: str) -> str:
        if self.tracing_enabled:
            tracing_state = 'enabled'
        else:
            tracing_state = 'disabled'
        patterns = {regex.pattern for regex, _ in self.regex_handlers}
        prefixes = {prefix for prefix, _ in self.prefix_handlers}
        handlers_list = '\n'.join(
            itertools.chain(
                (f'- {path}' for path in self.handlers),
                (f'- REGEX {pattern}' for pattern in patterns),
                (f'- PREFIX {prefix}' for prefix in prefixes),
            ),
        )
        return (
            f'Mockserver handler is not installed for {path!r}.\n\n'
            f'Perhaps you forgot to setup mockserver handler. '
            f'Tracing: {tracing_state}. Installed handlers:\n'
            f'{handlers_list}'
        )

    async def handle_request(
        self,
        request: MockserverRequest,
        nofail_404: bool,
    ):
        try:
            handler, kwargs = self._get_handler_for_request(request)
        except exceptions.HandlerNotFoundError as exc:
            if not nofail_404:
                self._asyncexc_append(exc)
            return _internal_error(f'Internal server error: {exc!r}')

        try:
            response = await handler(request, **kwargs)
            if isinstance(response, http.Response):
                return response.to_aiohttp()
            elif isinstance(
                response, (aiohttp.web.Response, aiohttp.web.WebSocketResponse)
            ):
                return response
            elif isinstance(response, http.MockedError):
                return _mocked_error_response(request, response.error_code)
            raise exceptions.MockServerError(
                'http.Response or aiohttp.web.Response instance is expected '
                f'{response!r} given',
            )
        except http.MockedError as exc:
            return _mocked_error_response(request, exc.error_code)
        except Exception as exc:
            self._asyncexc_append(exc)
            return _internal_error(f'Internal server error: {exc!r}')

    def register_handler(
        self,
        path: str,
        func,
        *,
        prefix: bool = False,
        regex: bool = False,
    ):
        if regex:
            if prefix:
                raise RuntimeError(
                    'Parameter value prefix=True is not supported if regex '
                    'parameter is also True.',
                )
            pattern = re.compile(path)
            self.regex_handlers.append((pattern, func))
        else:
            if prefix:
                self.prefix_handlers.append((path, func))
            else:
                self.handlers[path] = func
        return func

    def _get_handler_for_request(
        self,
        request: MockserverRequest,
    ) -> tuple[Handler, RouteParams]:
        path = request.original_path
        if self.http_proxy_enabled:
            host = request.headers.get('host')
            if host and host != self.mockserver_host:
                return self.get_handler(f'http://{host}{path}')
        return self.get_handler(path)

    def collect_calls(self) -> list[dict]:
        calls = []
        for _, handler in self.regex_handlers:
            calls.extend(handler.collect_calls())
        for _, handler in self.prefix_handlers:
            calls.extend(handler.collect_calls())
        for handler in self.handlers.values():
            calls.extend(handler.collect_calls())
        return calls


# pylint: disable=too-many-instance-attributes
class Server:
    session = None

    def __init__(
        self,
        mockserver_info: classes.MockserverInfo,
        *,
        nofail=False,
        mockserver_debug=False,
        tracing_enabled=True,
        trace_id_header=DEFAULT_TRACE_ID_HEADER,
        span_id_header=DEFAULT_SPAN_ID_HEADER,
        http_proxy_enabled=False,
    ):
        self._info = mockserver_info
        self._nofail = nofail
        self._mockserver_debug = mockserver_debug
        self._tracing_enabled = tracing_enabled
        self._trace_id_header = trace_id_header
        self._span_id_header = span_id_header
        self._http_proxy_enabled = http_proxy_enabled

    @property
    def tracing_enabled(self) -> bool:
        if self.session is None:
            return self._tracing_enabled
        return self.session.tracing_enabled

    @property
    def trace_id_header(self):
        return self._trace_id_header

    @property
    def span_id_header(self):
        return self._span_id_header

    @property
    def http_proxy_enabled(self):
        return self._http_proxy_enabled

    @property
    def server_info(self) -> classes.MockserverInfo:
        return self._info

    def get_debug(self) -> bool:
        return self._mockserver_debug

    def set_debug(self, enabled: bool):
        self._mockserver_debug = enabled

    @contextlib.contextmanager
    def new_session(
        self,
        *,
        asyncexc_append,
        traceid_manager: TraceidManager,
    ):
        self.session = Session(
            asyncexc_append=asyncexc_append,
            tracing_enabled=self._tracing_enabled,
            traceid_manager=traceid_manager,
            http_proxy_enabled=self._http_proxy_enabled,
            mockserver_host=self._info.get_host_header(),
        )
        try:
            yield self.session
        finally:
            self.session = None

    async def handle_request(self, request):
        started = time.perf_counter()
        try:
            response = await self._handle_request(request)
            self._log_request(started, request, response)
            return response
        except BaseException as exc:
            self._log_request(started, request, exc=exc)
            raise

    def _log_request(self, started, request, response=None, exc=None):
        if exc is None and not self._mockserver_debug:
            return
        fields = {
            '_type': 'mockserver_request',
            'timestamp': utils.utcnow(),
            'method': request.method,
            'url': request.rel_url,
        }
        for header, key in _LOGGER_HEADERS:
            if header in request.headers:
                fields[key] = request.headers[header]
        delay_ms = 1000 * (time.perf_counter() - started)
        fields['delay'] = f'{delay_ms:.3f}ms'
        if response is not None:
            log_level = logging.DEBUG
            fields['meta_code'] = response.status
            fields['status'] = 'DONE'
        else:
            log_level = logging.ERROR
            fields['status'] = 'FAIL'
            fields['exc_info'] = str(exc)
        logger.log(log_level, 'Mockserver request', extra={'tskv': fields})

    async def _handle_request(self, request: MockserverRequest):
        if not self.session:
            raise exceptions.MockServerError(
                'Internal error: mockserver session was not initialized'
            )

        nofail = self._nofail
        trace_id = request.headers.get(self.trace_id_header)
        traceid_manager = self.session.traceid_manager
        if self.tracing_enabled and not traceid_manager.is_testsuite(trace_id):
            nofail = True

        if self.tracing_enabled and traceid_manager.is_other_test(trace_id):
            self._report_other_test_request(request, trace_id)
            return _internal_error(REQUEST_FROM_ANOTHER_TEST_ERROR)
        try:
            return await self.session.handle_request(request, nofail_404=nofail)
        except exceptions.HandlerNotFoundError as exc:
            return _internal_error(
                'Internal error: mockserver handler not found',
            )

    def _report_other_test_request(self, request, trace_id):
        logger.warning(
            'Mockserver called path %s with previous test trace_id %s',
            request.path,
            trace_id,
        )


class MockserverFixture:
    """Mockserver handler installer fixture."""

    def __init__(
        self,
        mockserver: Server,
        session: Session,
        base_prefix: str = '',
        *,
        strict_default: bool = False,
    ) -> None:
        self._server = mockserver
        self._session = session
        self._base_prefix = base_prefix
        self._base_prefix_re = re.escape(base_prefix)
        self._strict_default = strict_default

    def new(self, prefix: str) -> 'MockserverFixture':
        """Create mockserver installer with given base prefix."""
        return MockserverFixture(
            self._server,
            self._session,
            self._build_fullpath(prefix),
        )

    @property
    def base_url(self) -> str:
        """Mockserver base url."""
        return self._server.server_info.base_url

    @property
    def host(self) -> str | None:
        """Mockserver hostname."""
        return self._server.server_info.host

    @property
    def port(self) -> int | None:
        """Mockserver port."""
        return self._server.server_info.port

    @property
    def trace_id_header(self) -> str:
        return self._server.trace_id_header

    @property
    def span_id_header(self) -> str:
        return self._server.span_id_header

    @property
    def trace_id(self) -> str:
        return self._session.traceid_manager.trace_id

    def handler(
        self,
        path: str,
        *,
        prefix: bool = False,
        raw_request: bool = False,
        json_response: bool = False,
        regex: bool = False,
        strict: typing.Optional[bool] = None,
    ) -> classes.GenericRequestDecorator:
        """Register basic http handler for ``path``.

        Returns decorator that registers handler ``path``. Original function is
        wrapped with :ref:`AsyncCallQueue`.

        :param path: match url by prefix if ``True`` exact match otherwise
        :param raw_request: pass ``aiohttp.web.Response`` to handler instead of
            ``testsuite.utils.http.Request``
        :param regex: set True to match path as regex pattern
        :param prefix: set True to match path prefix instead of whole path
        :param json_response: set True to let handler return json object
               instead of full response object

        .. code-block:: python

           @mockserver.handler('/service/path')
           def handler(request: testsuite.utils.http.Request):
               return mockserver.make_response('Hello, world!')
        """

        if raw_request:
            warnings.warn(
                'raw_request=True is deprecated, use aiohttp_handler() instead',
                DeprecationWarning,
            )
        if json_response:
            warnings.warn(
                'json_response=True is deprecated, use json_handler() instead',
                DeprecationWarning,
            )

        return self._handler_installer(
            path,
            prefix=prefix,
            raw_request=raw_request,
            json_response=json_response,
            regex=regex,
            strict=strict,
        )

    def json_handler(
        self,
        path: str,
        *,
        prefix: bool = False,
        raw_request: bool = False,
        regex: bool = False,
        strict: typing.Optional[bool] = None,
    ) -> classes.JsonRequestDecorator:
        """Register json http handler for ``path``.

        Returns decorator that registers handler ``path``. Original function is
        wrapped with :ref:`AsyncCallQueue`.

        :param path: match url by prefix if ``True`` exact match otherwise
        :param raw_request: pass ``aiohttp.web.Response`` to handler instead of
            ``testsuite.utils.http.Request``
        :param prefix: set True to match path prefix instead of whole path
        :param regex: set True to match path as regex pattern

        .. code-block:: python

           @mockserver.json_handler('/service/path')
           def handler(request: testsuite.utils.http.Request):
               # Return JSON document
               return {...}
               # or call to mockserver.make_response()
               return mockserver.make_response(...)
        """
        if raw_request:
            warnings.warn(
                'raw_request=True is deprecated, '
                'use aiohttp_json_handler() instead',
                DeprecationWarning,
            )
        return self._handler_installer(
            path,
            prefix=prefix,
            raw_request=raw_request,
            json_response=True,
            regex=regex,
            strict=strict,
        )

    def aiohttp_handler(
        self,
        path: str,
        *,
        prefix: bool = False,
        regex: bool = False,
        strict: typing.Optional[bool] = None,
    ) -> classes.GenericRequestDecorator:
        return self._handler_installer(
            path,
            prefix=prefix,
            raw_request=True,
            json_response=False,
            regex=regex,
            strict=strict,
        )

    def aiohttp_json_handler(
        self,
        path: str,
        *,
        prefix: bool = False,
        regex: bool = False,
        strict: typing.Optional[bool] = None,
    ) -> classes.JsonRequestDecorator:
        return self._handler_installer(
            path,
            prefix=prefix,
            raw_request=True,
            json_response=True,
            regex=regex,
            strict=strict,
        )

    def url(self, path: str) -> str:
        """Builds mockserver url for ``path``"""
        return url_util.join(self.base_url, path)

    def url_encoded(self, path: str) -> yarl.URL:
        """Builds mockserver url for ``path``"""
        return yarl.URL(url_util.join(self.base_url, path), encoded=True)

    def ws_url(self, path: str) -> str:
        return self._server.server_info.ws_url(path)

    def ignore_trace_id(self) -> typing.ContextManager[None]:
        return self.tracing(False)

    @contextlib.contextmanager
    def tracing(self, value: bool = True):
        original_value = self._session.tracing_enabled
        try:
            self._session.tracing_enabled = value
            yield
        finally:
            self._session.tracing_enabled = original_value

    def get_callqueue_for(self, path) -> callinfo.AsyncCallQueue:
        handler, _ = self._session.get_handler(path)
        return handler.callqueue

    make_response = staticmethod(http.make_response)

    TimeoutError = http.TimeoutError
    NetworkError = http.NetworkError

    def _handler_installer(
        self,
        path: str,
        *,
        strict: typing.Optional[bool],
        prefix: bool = False,
        raw_request: bool = False,
        json_response: bool = False,
        regex: bool = False,
    ) -> typing.Callable:
        path = self._build_fullpath(path, regex)
        if strict is None:
            strict = self._strict_default

        def decorator(func):
            handler = Handler(
                func,
                raw_request=raw_request,
                json_response=json_response,
                strict=strict,
            )
            self._session.register_handler(
                path,
                handler,
                prefix=prefix,
                regex=regex,
            )
            return handler.callqueue

        return decorator

    def _build_fullpath(self, path, regex: bool = False) -> str:
        if regex:
            return self._base_prefix_re + path
        if not self._base_prefix or self._base_prefix.endswith('/'):
            if self._server.http_proxy_enabled and path.startswith('http://'):
                return path
            return url_util.join(self._base_prefix, path)
        return self._base_prefix + path


MockserverSslFixture = MockserverFixture


def create_server(
    *,
    host: str,
    port: int,
    pytestconfig,
    ssl_info=None,
    loop=None,
):
    warnings.warn('Use mockserver_create() fixture instead', DeprecationWarning)

    mockserver_socket = _create_mockserver_socket(host=host, port=port)
    return _create_server_from_socket(
        mockserver_socket,
        mockserver_config=classes.MockserverConfig(),
        ssl_cert=ssl_info,
        loop=loop,
    )


def _create_ssl_context(ssl_cert: classes.SslCertInfo) -> ssl.SSLContext:
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(ssl_cert.cert_path, ssl_cert.private_key_path)
    return ssl_context


def _internal_error(message: str = 'Internal error') -> aiohttp.web.Response:
    return http.make_response(message, status=500).to_aiohttp()


def _mocked_error_response(request, error_code) -> aiohttp.web.Response:
    if _SUPPORTED_ERRORS_HEADER not in request.headers:
        raise exceptions.MockServerError(
            'Service does not support mockserver errors protocol',
        )
    supported_errors = request.headers[_SUPPORTED_ERRORS_HEADER].split(',')
    if error_code not in supported_errors:
        raise exceptions.MockServerError(
            f'Service does not support mockserver error of type {error_code}',
        )
    return http.make_response(
        response='',
        status=599,
        headers={_ERROR_HEADER: error_code},
    ).to_aiohttp()


def _create_server_obj(
    mockserver_info: classes.MockserverInfo,
    mockserver_config: classes.MockserverConfig,
) -> Server:
    return Server(
        mockserver_info,
        nofail=mockserver_config.nofail,
        mockserver_debug=mockserver_config.debug,
        tracing_enabled=mockserver_config.tracing_enabled,
        trace_id_header=mockserver_config.trace_id_header,
        span_id_header=mockserver_config.span_id_header,
        http_proxy_enabled=mockserver_config.http_proxy_enabled,
    )


def _create_web_server(server: Server, loop) -> aiohttp.web.Server:
    def request_factory(*args):
        return MockserverRequest(*args, loop=loop)

    return aiohttp.web.Server(
        server.handle_request,
        request_factory=request_factory,
        loop=loop,
        access_log=None,
    )


def _create_mockserver_socket(
    socket_path=None,
    host='localhost',
    port=0,
    https=False,
):
    if socket_path is None:
        sockets = net_utils.bind_socket_multiple(host, port)
    else:
        sockets = [net_utils.bind_unix_socket(socket_path)]
    assert sockets
    for sock in sockets:
        sock.setblocking(False)
    info = _create_mockserver_info(
        sockets[0],
        socket_path=socket_path,
        host=host,
        https=https,
    )
    return classes.MockserverSocket(sockets=sockets, info=info)


@contextlib.asynccontextmanager
async def _create_server_from_socket(
    mockserver_socket: classes.MockserverSocket,
    mockserver_config: classes.MockserverConfig,
    ssl_cert: classes.SslCertInfo | None = None,
    loop=None,
) -> typing.AsyncGenerator[Server, None]:
    if ssl_cert:
        ssl_context = _create_ssl_context(ssl_cert)
    else:
        ssl_context = None

    if loop is None:
        loop = asyncio.get_running_loop()

    server = _create_server_obj(mockserver_socket.info, mockserver_config)
    web_server = _create_web_server(server, loop)

    async with net_utils.create_server_multiple(
        web_server,
        sockets=mockserver_socket.sockets,
        ssl=ssl_context,
    ) as aio_server:
        yield server


def _create_mockserver_info(
    sock,
    socket_path,
    host: str,
    https: bool = False,
) -> classes.MockserverInfo:
    if socket_path:
        return _create_unix_mockserver_info(socket_path)
    port = sock.getsockname()[1]
    schema = 'https' if https else 'http'
    base_url = f'{schema}://{host}:{port}/'
    return classes.MockserverInfo(
        host=host,
        port=port,
        base_url=base_url,
        https=https,
    )


def _create_unix_mockserver_info(
    socket_path: pathlib.Path,
) -> classes.MockserverInfo:
    return classes.MockserverInfo(
        socket_path=socket_path,
        # use localhost to avoid aiohttp complains on invalid url
        base_url='http://localhost/',
        host='localhost',
        port=80,
        https=False,
    )


def _path_from_message(message):
    """Returns original HTTP path without query."""
    path = str(message.url)
    path = path.split('?')[0]
    path = urllib.parse.unquote(path)
    return path
