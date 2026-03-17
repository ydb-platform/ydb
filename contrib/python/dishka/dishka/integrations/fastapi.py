__all__ = [
    "DishkaRoute",
    "DishkaSyncRoute",
    "FastapiProvider",
    "FromDishka",
    "inject",
    "inject_sync",
    "setup_dishka",
]

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from inspect import (
    Parameter,
    isasyncgenfunction,
    iscoroutinefunction,
    signature,
)
from typing import Annotated, Any, ParamSpec, TypeVar, get_type_hints

from fastapi import Depends, FastAPI, Request, WebSocket
from fastapi.routing import APIRoute

from dishka import (
    AsyncContainer,
    Container,
    DependencyKey,
    FromDishka,
    Provider,
    Scope,
    from_context,
)
from .base import default_parse_dependency, wrap_injection
from .starlette import ContainerMiddleware, SyncContainerMiddleware

T = TypeVar("T")
P = ParamSpec("P")


DISHKA_REQUEST_PARAM = Parameter(
    name="___dishka_request",
    annotation=Request,
    kind=Parameter.KEYWORD_ONLY,
)
DISHKA_WEBSOCKET_PARAM = Parameter(
    name="___dishka_websocket",
    annotation=WebSocket,
    kind=Parameter.KEYWORD_ONLY,
)


class ContainerSource(Enum):
    REQUEST = "request"
    WEBSOCKET = "websocket"


@dataclass(frozen=True)
class ContainerResult:
    container: AsyncContainer | Container
    source: ContainerSource | None = None


def _async_depends(dependency: DependencyKey) -> Any:
    async def dishka_depends(request: Request) -> dependency:
        return await request.state.dishka_container.get(
            dependency.type_hint,
            component=dependency.component,
        )

    return Annotated[dependency.type_hint, Depends(dishka_depends)]


def _replace_depends(
    func: Callable[P, T],
    depends_factory: Callable[[DependencyKey], Any],
) -> Callable[P, T]:
    hints = get_type_hints(func, include_extras=True)
    func_signature = signature(func)

    new_params = []
    for name, param in func_signature.parameters.items():
        hint = hints.get(name, Any)
        dep = default_parse_dependency(param, hint)
        if dep is None:
            new_params.append(param)
            continue
        new_dep = depends_factory(dep)
        hints[name] = new_dep
        new_params.append(param.replace(annotation=new_dep))
    func.__signature__ = func_signature.replace(parameters=new_params)
    func.__annotations__ = hints
    return func


def _find_context_param(func: Callable[P, T]) -> str | None:
    hints = get_type_hints(func, include_extras=True)
    func_signature = signature(func)

    request_hint = None
    websocket_hint = None

    for name, hint in hints.items():
        param = func_signature.parameters.get(name)
        if param is None:
            continue
        if default_parse_dependency(param, hint) is not None:
            continue
        if hint is Request:
            request_hint = name
        elif hint is WebSocket:
            websocket_hint = name

    return request_hint or websocket_hint


def _get_container_with_source(
    _: tuple,
    kwargs: dict,
    *,
    param_name: str | None,
) -> ContainerResult:
    if param_name and param_name in kwargs:
        return ContainerResult(
            container=kwargs[param_name].state.dishka_container,
        )

    if DISHKA_REQUEST_PARAM.name in kwargs:
        return ContainerResult(
            container=kwargs[DISHKA_REQUEST_PARAM.name].state.dishka_container,
            source=ContainerSource.REQUEST,
        )

    return ContainerResult(
        container=kwargs[DISHKA_WEBSOCKET_PARAM.name].state.dishka_container,
        source=ContainerSource.WEBSOCKET,
    )


def _get_additional_params(
    param_name: str | None,
    source: ContainerSource,
) -> list[Parameter]:
    if param_name:
        return []

    if source == ContainerSource.REQUEST:
        return [DISHKA_REQUEST_PARAM]

    return [DISHKA_WEBSOCKET_PARAM]


def _wrap_fastapi_injection(
    *,
    func: Callable[P, T],
    is_async: bool,
) -> Callable[P, T]:
    param_name = _find_context_param(func)

    additional_params = (
        [] if param_name else [DISHKA_REQUEST_PARAM, DISHKA_WEBSOCKET_PARAM]
    )

    def container_getter(
        args: tuple,
        kwargs: dict,
    ) -> AsyncContainer | Container:
        result = _get_container_with_source(
            args,
            kwargs,
            param_name=param_name,
        )

        additional_params[:] = _get_additional_params(
            param_name,
            result.source,
        )

        return result.container

    return wrap_injection(
        func=func,
        is_async=is_async,
        additional_params=additional_params,
        container_getter=container_getter,
    )


def inject(func: Callable[P, T]) -> Callable[P, T]:
    if not iscoroutinefunction(func) and not isasyncgenfunction(func):
        return _replace_depends(func, _async_depends)
    return _wrap_fastapi_injection(func=func, is_async=True)


def inject_sync(func: Callable[P, T]) -> Callable[P, T]:
    return _wrap_fastapi_injection(func=func, is_async=False)


class DishkaRoute(APIRoute):
    def __init__(
        self,
        path: str,
        endpoint: Callable[..., Any],
        **kwargs: Any,
    ) -> None:
        endpoint = inject(endpoint)
        super().__init__(path, endpoint, **kwargs)


class DishkaSyncRoute(APIRoute):
    def __init__(
        self,
        path: str,
        endpoint: Callable[..., Any],
        **kwargs: Any,
    ) -> None:
        endpoint = inject_sync(endpoint)
        super().__init__(path, endpoint, **kwargs)


class FastapiProvider(Provider):
    request = from_context(Request, scope=Scope.REQUEST)
    websocket = from_context(WebSocket, scope=Scope.SESSION)


def setup_dishka(container: AsyncContainer | Container, app: FastAPI) -> None:
    if isinstance(container, AsyncContainer):
        app.add_middleware(ContainerMiddleware)
    else:
        app.add_middleware(SyncContainerMiddleware)
    app.state.dishka_container = container
