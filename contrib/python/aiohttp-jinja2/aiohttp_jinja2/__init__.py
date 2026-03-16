import functools
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Final,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)

import jinja2
from aiohttp import web
from aiohttp.abc import AbstractView

from .helpers import GLOBAL_HELPERS, static_root_key
from .typedefs import Filters

__version__ = "1.6"

__all__ = (
    "get_env",
    "render_string",
    "render_template",
    "setup",
    "static_root_key",
    "template",
)

_TemplateReturnType = Awaitable[Union[web.StreamResponse, Mapping[str, Any]]]
_SimpleTemplateHandler = Callable[[web.Request], _TemplateReturnType]
_ContextProcessor = Callable[[web.Request], Awaitable[Dict[str, Any]]]

APP_CONTEXT_PROCESSORS_KEY: Final = web.AppKey[Sequence[_ContextProcessor]](
    "APP_CONTEXT_PROCESSORS_KEY"
)
APP_KEY: Final = web.AppKey[jinja2.Environment]("APP_KEY")
REQUEST_CONTEXT_KEY: Final = "aiohttp_jinja2_context"

_T = TypeVar("_T")
_AbstractView = TypeVar("_AbstractView", bound=AbstractView)


class _TemplateWrapper(Protocol):
    @overload
    def __call__(
        self, func: _SimpleTemplateHandler
    ) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
        ...

    @overload
    def __call__(
        self, func: Callable[[_AbstractView], _TemplateReturnType]
    ) -> Callable[[_AbstractView], Awaitable[web.StreamResponse]]:
        ...

    @overload
    def __call__(
        self, func: Callable[[_T, web.Request], _TemplateReturnType]
    ) -> Callable[[_T, web.Request], Awaitable[web.StreamResponse]]:
        ...


def setup(
    app: web.Application,
    *args: Any,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
    context_processors: Sequence[_ContextProcessor] = (),
    filters: Optional[Filters] = None,
    default_helpers: bool = True,
    **kwargs: Any,
) -> jinja2.Environment:
    kwargs.setdefault("autoescape", True)
    env = jinja2.Environment(*args, **kwargs)
    if default_helpers:
        env.globals.update(GLOBAL_HELPERS)
    if filters is not None:
        env.filters.update(filters)
    app[app_key] = env
    if context_processors:
        app[APP_CONTEXT_PROCESSORS_KEY] = context_processors
        app.middlewares.append(context_processors_middleware)

    env.globals["app"] = app

    return env


def get_env(
    app: web.Application, *, app_key: web.AppKey[jinja2.Environment] = APP_KEY
) -> jinja2.Environment:
    try:
        return app[app_key]
    except KeyError:
        raise RuntimeError("aiohttp_jinja2.setup(...) must be called first.")


def _render_string(
    template_name: str,
    request: web.Request,
    context: Mapping[str, Any],
    app_key: web.AppKey[jinja2.Environment],
) -> Tuple[jinja2.Template, Mapping[str, Any]]:
    env = request.config_dict.get(app_key)
    if env is None:
        text = "Template engine is not initialized, call aiohttp_jinja2.setup() first"
        # in order to see meaningful exception message both: on console
        # output and rendered page we add same message to *reason* and
        # *text* arguments.
        raise web.HTTPInternalServerError(reason=text, text=text)
    try:
        template = env.get_template(template_name)
    except jinja2.TemplateNotFound as e:
        text = f"Template '{template_name}' not found"
        raise web.HTTPInternalServerError(reason=text, text=text) from e
    if not isinstance(context, Mapping):
        text = f"context should be mapping, not {type(context)}"  # type: ignore[unreachable]
        # same reason as above
        raise web.HTTPInternalServerError(reason=text, text=text)
    if request.get(REQUEST_CONTEXT_KEY):
        context = dict(request[REQUEST_CONTEXT_KEY], **context)
    return template, context


def render_string(
    template_name: str,
    request: web.Request,
    context: Mapping[str, Any],
    *,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
) -> str:
    template, context = _render_string(template_name, request, context, app_key)
    return template.render(context)


async def render_string_async(
    template_name: str,
    request: web.Request,
    context: Mapping[str, Any],
    *,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
) -> str:
    template, context = _render_string(template_name, request, context, app_key)
    return await template.render_async(context)


def _render_template(
    context: Optional[Mapping[str, Any]],
    encoding: str,
    status: int,
) -> Tuple[web.Response, Mapping[str, Any]]:
    response = web.Response(status=status)
    if context is None:
        context = {}
    response.content_type = "text/html"
    response.charset = encoding
    return response, context


def render_template(
    template_name: str,
    request: web.Request,
    context: Optional[Mapping[str, Any]],
    *,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
    encoding: str = "utf-8",
    status: int = 200,
) -> web.Response:
    response, context = _render_template(context, encoding, status)
    response.text = render_string(template_name, request, context, app_key=app_key)
    return response


async def render_template_async(
    template_name: str,
    request: web.Request,
    context: Optional[Mapping[str, Any]],
    *,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
    encoding: str = "utf-8",
    status: int = 200,
) -> web.Response:
    response, context = _render_template(context, encoding, status)
    response.text = await render_string_async(
        template_name, request, context, app_key=app_key
    )
    return response


def template(
    template_name: str,
    *,
    app_key: web.AppKey[jinja2.Environment] = APP_KEY,
    encoding: str = "utf-8",
    status: int = 200,
) -> _TemplateWrapper:
    @overload
    def wrapper(
        func: _SimpleTemplateHandler,
    ) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
        ...

    @overload
    def wrapper(
        func: Callable[[_AbstractView], _TemplateReturnType]
    ) -> Callable[[_AbstractView], Awaitable[web.StreamResponse]]:
        ...

    @overload
    def wrapper(
        func: Callable[[_T, web.Request], _TemplateReturnType]
    ) -> Callable[[_T, web.Request], Awaitable[web.StreamResponse]]:
        ...

    def wrapper(
        func: Callable[..., _TemplateReturnType]
    ) -> Callable[..., Awaitable[web.StreamResponse]]:
        # TODO(PY310): ParamSpec

        @functools.wraps(func)
        async def wrapped(*args: Any) -> web.StreamResponse:  # type: ignore[misc]
            context = await func(*args)
            if isinstance(context, web.StreamResponse):
                return context

            # Supports class based views see web.View
            if isinstance(args[0], AbstractView):
                request = args[0].request
            else:
                request = args[-1]

            env = request.config_dict.get(app_key)
            if env and env.is_async:
                response = await render_template_async(
                    template_name, request, context, app_key=app_key, encoding=encoding
                )
            else:
                response = render_template(
                    template_name, request, context, app_key=app_key, encoding=encoding
                )
            response.set_status(status)
            return response

        return wrapped

    return wrapper


@web.middleware
async def context_processors_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    if REQUEST_CONTEXT_KEY not in request:
        request[REQUEST_CONTEXT_KEY] = {}
    for processor in request.config_dict[APP_CONTEXT_PROCESSORS_KEY]:
        request[REQUEST_CONTEXT_KEY].update(await processor(request))
    return await handler(request)


async def request_processor(request: web.Request) -> Dict[str, web.Request]:
    return {"request": request}
