from __future__ import annotations

import os
from datetime import datetime, timedelta
from http.cookiejar import CookieJar
from types import TracebackType
from typing import (
    Any,
    AnyStr,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

from hypercorn.typing import (
    ASGIReceiveCallable,
    ASGISendCallable,
    HTTPScope,
    LifespanScope,
    WebsocketScope,
)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

if TYPE_CHECKING:
    from werkzeug.datastructures import Headers  # noqa: F401
    from werkzeug.wrappers import Response as WerkzeugResponse

    from .app import Quart
    from .sessions import SessionMixin
    from .wrappers.response import Response  # noqa: F401

FilePath = Union[bytes, str, os.PathLike]

# The possible types that are directly convertible or are a Response object.
ResponseValue = Union[
    "Response",
    "WerkzeugResponse",
    AnyStr,
    Dict[str, Any],  # any jsonify-able dict
    AsyncGenerator[AnyStr, None],
    Generator[AnyStr, None, None],
]
StatusCode = int

# the possible types for an individual HTTP header
HeaderName = str
HeaderValue = Union[str, List[str], Tuple[str, ...]]

# the possible types for HTTP headers
HeadersValue = Union["Headers", Dict[HeaderName, HeaderValue], List[Tuple[HeaderName, HeaderValue]]]

# The possible types returned by a route function.
ResponseReturnValue = Union[
    ResponseValue,
    Tuple[ResponseValue, HeadersValue],
    Tuple[ResponseValue, StatusCode],
    Tuple[ResponseValue, StatusCode, HeadersValue],
]

AppOrBlueprintKey = Optional[str]  # The App key is None, whereas blueprints are named
AfterRequestCallable = Union[
    Callable[["Response"], "Response"], Callable[["Response"], Awaitable["Response"]]
]
AfterServingCallable = Union[Callable[[], None], Callable[[], Awaitable[None]]]
AfterWebsocketCallable = Union[
    Callable[["Response"], Optional["Response"]],
    Callable[["Response"], Awaitable[Optional["Response"]]],
]
BeforeFirstRequestCallable = Union[Callable[[], None], Callable[[], Awaitable[None]]]
BeforeRequestCallable = Union[
    Callable[[], Optional[ResponseReturnValue]],
    Callable[[], Awaitable[Optional[ResponseReturnValue]]],
]
BeforeServingCallable = Union[Callable[[], None], Callable[[], Awaitable[None]]]
BeforeWebsocketCallable = Union[
    Callable[[], Optional[ResponseReturnValue]],
    Callable[[], Awaitable[Optional[ResponseReturnValue]]],
]
ErrorHandlerCallable = Union[
    Callable[[Exception], ResponseReturnValue],
    Callable[[Exception], Awaitable[ResponseReturnValue]],
]
TeardownCallable = Union[
    Callable[[Optional[BaseException]], None],
    Callable[[Optional[BaseException]], Awaitable[None]],
]
TemplateContextProcessorCallable = Union[
    Callable[[], Dict[str, Any]], Callable[[], Awaitable[Dict[str, Any]]]
]
TemplateFilterCallable = Callable[[Any], Any]
TemplateGlobalCallable = Callable[[Any], Any]
TemplateTestCallable = Callable[[Any], bool]
URLDefaultCallable = Callable[[str, dict], None]
URLValuePreprocessorCallable = Callable[[Optional[str], Optional[dict]], None]


class ASGIHTTPProtocol(Protocol):
    def __init__(self, app: Quart, scope: HTTPScope) -> None:
        ...

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        ...


class ASGILifespanProtocol(Protocol):
    def __init__(self, app: Quart, scope: LifespanScope) -> None:
        ...

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        ...


class ASGIWebsocketProtocol(Protocol):
    def __init__(self, app: Quart, scope: WebsocketScope) -> None:
        ...

    async def __call__(self, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        ...


class TestHTTPConnectionProtocol(Protocol):
    push_promises: List[Tuple[str, Headers]]

    def __init__(self, app: Quart, scope: HTTPScope, _preserve_context: bool = False) -> None:
        ...

    async def send(self, data: bytes) -> None:
        ...

    async def send_complete(self) -> None:
        ...

    async def receive(self) -> bytes:
        ...

    async def disconnect(self) -> None:
        ...

    async def __aenter__(self) -> TestHTTPConnectionProtocol:
        ...

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        ...

    async def as_response(self) -> Response:
        ...


class TestWebsocketConnectionProtocol(Protocol):
    def __init__(self, app: Quart, scope: WebsocketScope) -> None:
        ...

    async def __aenter__(self) -> TestWebsocketConnectionProtocol:
        ...

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        ...

    async def receive(self) -> AnyStr:
        ...

    async def send(self, data: AnyStr) -> None:
        ...

    async def receive_json(self) -> Any:
        ...

    async def send_json(self, data: Any) -> None:
        ...

    async def close(self, code: int) -> None:
        ...

    async def disconnect(self) -> None:
        ...


class TestClientProtocol(Protocol):
    app: Quart
    cookie_jar: Optional[CookieJar]
    http_connection_class: Type[TestHTTPConnectionProtocol]
    push_promises: List[Tuple[str, Headers]]
    websocket_connection_class: Type[TestWebsocketConnectionProtocol]

    def __init__(self, app: Quart, use_cookies: bool = True) -> None:
        ...

    async def open(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: Optional[Union[dict, Headers]] = None,
        data: Optional[AnyStr] = None,
        form: Optional[dict] = None,
        query_string: Optional[dict] = None,
        json: Any = None,
        scheme: str = "http",
        follow_redirects: bool = False,
        root_path: str = "",
        http_version: str = "1.1",
    ) -> Response:
        ...

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: Optional[Union[dict, Headers]] = None,
        query_string: Optional[dict] = None,
        scheme: str = "http",
        root_path: str = "",
        http_version: str = "1.1",
    ) -> TestHTTPConnectionProtocol:
        ...

    def websocket(
        self,
        path: str,
        *,
        headers: Optional[Union[dict, Headers]] = None,
        query_string: Optional[dict] = None,
        scheme: str = "ws",
        subprotocols: Optional[List[str]] = None,
        root_path: str = "",
        http_version: str = "1.1",
    ) -> TestWebsocketConnectionProtocol:
        ...

    async def delete(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def get(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def head(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def options(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def patch(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def post(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def put(self, *args: Any, **kwargs: Any) -> Response:
        ...

    async def trace(self, *args: Any, **kwargs: Any) -> Response:
        ...

    def set_cookie(
        self,
        server_name: str,
        key: str,
        value: str = "",
        max_age: Optional[Union[int, timedelta]] = None,
        expires: Optional[Union[int, float, datetime]] = None,
        path: str = "/",
        domain: Optional[str] = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: str = None,
        charset: str = "utf-8",
    ) -> None:
        ...

    def delete_cookie(
        self, server_name: str, key: str, path: str = "/", domain: Optional[str] = None
    ) -> None:
        ...

    def session_transaction(
        self,
        path: str = "/",
        *,
        method: str = "GET",
        headers: Optional[Union[dict, Headers]] = None,
        query_string: Optional[dict] = None,
        scheme: str = "http",
        data: Optional[AnyStr] = None,
        form: Optional[dict] = None,
        json: Any = None,
        root_path: str = "",
        http_version: str = "1.1",
    ) -> AsyncContextManager[SessionMixin]:
        ...

    async def __aenter__(self) -> TestClientProtocol:
        ...

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        ...


class TestAppProtocol(Protocol):
    def __init__(self, app: Quart) -> None:
        ...

    def test_client(self) -> TestClientProtocol:
        ...

    async def startup(self) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    async def __aenter__(self) -> TestAppProtocol:
        ...

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        ...
