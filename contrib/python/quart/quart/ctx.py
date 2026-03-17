from __future__ import annotations

import sys
from functools import wraps
from types import TracebackType
from typing import Any, Callable, cast, Iterator, List, Optional, TYPE_CHECKING  # noqa: F401

from werkzeug.exceptions import HTTPException

from .globals import _app_ctx_stack, _request_ctx_stack, _websocket_ctx_stack
from .sessions import SessionMixin  # noqa
from .signals import appcontext_popped, appcontext_pushed
from .typing import AfterRequestCallable, AfterWebsocketCallable
from .wrappers import BaseRequestWebsocket, Request, Websocket

if TYPE_CHECKING:
    from .app import Quart  # noqa

_sentinel = object()


class _BaseRequestWebsocketContext:
    """A base context relating to either request or websockets, bound to the current task.

    Attributes:
        app: The app itself.
        request_websocket: The request or websocket itself.
        url_adapter: An adapter bound to this request.
        session: The session information relating to this request.
    """

    def __init__(
        self,
        app: "Quart",
        request_websocket: BaseRequestWebsocket,
        session: Optional[SessionMixin] = None,
    ) -> None:
        self.app = app
        self.request_websocket = request_websocket
        self.url_adapter = app.create_url_adapter(self.request_websocket)
        self.request_websocket.routing_exception = None
        self.session = session
        self.preserved = False
        self._implicit_app_ctx_stack: List[Optional[AppContext]] = []

    @property
    def g(self) -> AppContext:
        return _app_ctx_stack.top.g

    @g.setter
    def g(self, value: AppContext) -> None:
        _app_ctx_stack.top.g = value

    def copy(self) -> "_BaseRequestWebsocketContext":
        return self.__class__(self.app, self.request_websocket, self.session)

    def match_request(self) -> None:
        """Match the request against the adapter.

        Override this method to configure request matching, it should
        set the request url_rule and view_args and optionally a
        routing_exception.
        """
        try:
            (
                self.request_websocket.url_rule,
                self.request_websocket.view_args,
            ) = self.url_adapter.match(  # type: ignore
                return_rule=True
            )  # noqa
        except HTTPException as exception:
            self.request_websocket.routing_exception = exception

    async def push(self) -> None:
        raise NotImplementedError()

    async def pop(self, exc: Optional[BaseException]) -> None:
        raise NotImplementedError()

    async def auto_pop(self, exc: Optional[BaseException]) -> None:
        if self.request_websocket.scope.get("_quart._preserve_context", False) or (
            exc is not None and self.app.preserve_context_on_exception
        ):
            self.preserved = True
        else:
            await self.pop(exc)

    async def __aenter__(self) -> "_BaseRequestWebsocketContext":
        await self.push()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.auto_pop(exc_value)

    async def _push_appctx(self) -> None:
        app_ctx = _app_ctx_stack.top
        if app_ctx is None or app_ctx.app != self.app:
            app_ctx = self.app.app_context()
            await app_ctx.push()
            self._implicit_app_ctx_stack.append(app_ctx)
        else:
            self._implicit_app_ctx_stack.append(None)

    async def _push(self) -> None:
        if self.session is None:
            session_interface = self.app.session_interface
            self.session = await self.app.ensure_async(session_interface.open_session)(
                self.app, self.request_websocket
            )

            if self.session is None:
                self.session = await session_interface.make_null_session(self.app)

        if self.url_adapter is not None:
            self.match_request()


class RequestContext(_BaseRequestWebsocketContext):
    """The context relating to the specific request, bound to the current task.

    Do not use directly, prefer the
    :func:`~quart.Quart.request_context` or
    :func:`~quart.Quart.test_request_context` instead.

    Attributes:
        _after_request_functions: List of functions to execute after the current
            request, see :func:`after_this_request`.
    """

    def __init__(
        self,
        app: "Quart",
        request: Request,
        session: Optional[SessionMixin] = None,
    ) -> None:
        super().__init__(app, request, session)
        self.flashes = None
        self._after_request_functions: List[AfterRequestCallable] = []

    @property
    def request(self) -> Request:
        return cast(Request, self.request_websocket)

    async def push(self) -> None:
        await super()._push_appctx()
        _request_ctx_stack.push(self)
        await super()._push()

    async def pop(self, exc: Optional[BaseException] = _sentinel) -> None:  # type: ignore
        app_ctx = self._implicit_app_ctx_stack.pop()
        try:
            if not self._implicit_app_ctx_stack:
                self.preserved = False
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                await self.app.do_teardown_request(exc, self)
        finally:
            _request_ctx_stack.pop()
            if app_ctx is not None:
                await app_ctx.pop(exc)

    async def __aenter__(self) -> "RequestContext":
        await self.push()
        return self


class WebsocketContext(_BaseRequestWebsocketContext):
    """The context relating to the specific websocket, bound to the current task.

    Do not use directly, prefer the
    :func:`~quart.Quart.websocket_context` or
    :func:`~quart.Quart.test_websocket_context` instead.

    Attributes:
        _after_websocket_functions: List of functions to execute after the current
            websocket, see :func:`after_this_websocket`.
    """

    def __init__(
        self,
        app: "Quart",
        request: Websocket,
        session: Optional[SessionMixin] = None,
    ) -> None:
        super().__init__(app, request, session)
        self._after_websocket_functions: List[AfterWebsocketCallable] = []

    @property
    def websocket(self) -> Websocket:
        return cast(Websocket, self.request_websocket)

    async def push(self) -> None:
        await super()._push_appctx()
        _websocket_ctx_stack.push(self)
        await super()._push()

    async def pop(self, exc: Optional[BaseException] = _sentinel) -> None:  # type: ignore
        app_ctx = self._implicit_app_ctx_stack.pop()
        try:
            if not self._implicit_app_ctx_stack:
                self.preserved = False
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                await self.app.do_teardown_websocket(exc, self)
        finally:
            _websocket_ctx_stack.pop()
            if app_ctx is not None:
                await app_ctx.pop(exc)

    async def __aenter__(self) -> "WebsocketContext":
        await self.push()
        return self


class AppContext:

    """The context relating to the app bound to the current task.

    Do not use directly, prefer the
    :func:`~quart.Quart.app_context` instead.

    Attributes:
        app: The app itself.
        url_adapter: An adapter bound to the server, but not a
            specific task, useful for route building.
        g: An instance of the ctx globals class.
    """

    def __init__(self, app: "Quart") -> None:
        self.app = app
        self.url_adapter = app.create_url_adapter(None)
        self.g = app.app_ctx_globals_class()
        self._app_reference_count = 0

    def copy(self) -> "AppContext":
        app_context = self.__class__(self.app)
        app_context.g = self.g
        return app_context

    async def push(self) -> None:
        self._app_reference_count += 1
        _app_ctx_stack.push(self)
        await appcontext_pushed.send(self.app)

    async def pop(self, exc: Optional[BaseException] = _sentinel) -> None:  # type: ignore
        self._app_reference_count -= 1
        try:
            if self._app_reference_count <= 0:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                await self.app.do_teardown_appcontext(exc)
        finally:
            _app_ctx_stack.pop()
        await appcontext_popped.send(self.app)

    async def __aenter__(self) -> "AppContext":
        await self.push()
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.pop(exc_value)


def after_this_request(func: AfterRequestCallable) -> AfterRequestCallable:
    """Schedule the func to be called after the current request.

    This is useful in situations whereby you want an after request
    function for a specific route or circumstance only, for example,

    .. code-block:: python

        def index():
            @after_this_request
            def set_cookie(response):
                response.set_cookie('special', 'value')
                return response

            ...
    """
    _request_ctx_stack.top._after_request_functions.append(func)
    return func


def after_this_websocket(func: AfterWebsocketCallable) -> AfterWebsocketCallable:
    """Schedule the func to be called after the current websocket.

    This is useful in situations whereby you want an after websocket
    function for a specific route or circumstance only, for example,

    .. note::
        The response is an optional argument, and will only be
        passed if the websocket was not active (i.e. there was an
        error).

    .. code-block:: python

        def index():
            @after_this_websocket
            def set_cookie(response: Optional[Response]):
                response.set_cookie('special', 'value')
                return response

            ...

    """
    _websocket_ctx_stack.top._after_websocket_functions.append(func)
    return func


def copy_current_app_context(func: Callable) -> Callable:
    """Share the current app context with the function decorated.

    The app context is local per task and hence will not be available
    in any other task. This decorator can be used to make the context
    available,

    .. code-block:: python

        @copy_current_app_context
        async def within_context() -> None:
            name = current_app.name
            ...

    """
    if not has_app_context():
        raise RuntimeError("Attempt to copy app context outside of a app context")

    app_context = _app_ctx_stack.top.copy()

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with app_context:
            return await app_context.app.ensure_async(func)(*args, **kwargs)

    return wrapper


def copy_current_request_context(func: Callable) -> Callable:
    """Share the current request context with the function decorated.

    The request context is local per task and hence will not be
    available in any other task. This decorator can be used to make
    the context available,

    .. code-block:: python

        @copy_current_request_context
        async def within_context() -> None:
            method = request.method
            ...

    """
    if not has_request_context():
        raise RuntimeError("Attempt to copy request context outside of a request context")

    request_context = _request_ctx_stack.top.copy()

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with request_context:
            return await request_context.app.ensure_async(func)(*args, **kwargs)

    return wrapper


def copy_current_websocket_context(func: Callable) -> Callable:
    """Share the current websocket context with the function decorated.

    The websocket context is local per task and hence will not be
    available in any other task. This decorator can be used to make
    the context available,

    .. code-block:: python

        @copy_current_websocket_context
        async def within_context() -> None:
            method = websocket.method
            ...

    """
    if not has_websocket_context():
        raise RuntimeError("Attempt to copy websocket context outside of a websocket context")

    websocket_context = _websocket_ctx_stack.top.copy()

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with websocket_context:
            return await websocket_context.app.ensure_async(func)(*args, **kwargs)

    return wrapper


def has_app_context() -> bool:
    """Check if execution is within an app context.

    This allows a controlled way to act if there is an app context
    available, or silently not act if not. For example,

    .. code-block:: python

        if has_app_context():
            log.info("Executing in %s context", current_app.name)

    See also :func:`has_request_context`
    """
    return _app_ctx_stack.top is not None


def has_request_context() -> bool:
    """Check if execution is within a request context.

    This allows a controlled way to act if there is a request context
    available, or silently not act if not. For example,

    .. code-block:: python

        if has_request_context():
            log.info("Request endpoint %s", request.endpoint)

    See also :func:`has_app_context`.
    """
    return _request_ctx_stack.top is not None


def has_websocket_context() -> bool:
    """Check if execution is within a websocket context.

    This allows a controlled way to act if there is a websocket
    context available, or silently not act if not. For example,

    .. code-block:: python

        if has_websocket_context():
            log.info("Websocket endpoint %s", websocket.endpoint)

    See also :func:`has_app_context`.
    """
    return _websocket_ctx_stack.top is not None


class _AppCtxGlobals:
    """The g class, a plain object with some mapping methods."""

    def get(self, name: str, default: Optional[Any] = None) -> Any:
        """Get a named attribute of this instance, or return the default."""
        return self.__dict__.get(name, default)

    def pop(self, name: str, default: Any = _sentinel) -> Any:
        """Pop, get and remove the named attribute of this instance."""
        if default is _sentinel:
            return self.__dict__.pop(name)
        else:
            return self.__dict__.pop(name, default)

    def setdefault(self, name: str, default: Any = None) -> Any:
        """Set an attribute with a default value."""
        return self.__dict__.setdefault(name, default)

    def __contains__(self, item: Any) -> bool:
        return item in self.__dict__

    def __iter__(self) -> Iterator:
        return iter(self.__dict__)

    def __repr__(self) -> str:
        top = _app_ctx_stack.top
        if top is not None:
            return f"<quart.g of {top.app.name}>"
        return object.__repr__(self)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__dict__[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name: str, value: Any) -> None:
        self.__dict__[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self.__dict__[name]
        except KeyError:
            raise AttributeError(name) from None
