from functools import cache

__all__ = [
    "add_pagination",
    "apply_items_transformer",
    "create_page",
    "pagination_ctx",
    "pagination_items",
    "request",
    "resolve_items_transformer",
    "resolve_page",
    "resolve_params",
    "response",
    "set_items_transformer",
    "set_page",
    "set_params",
]

import inspect
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextlib import AbstractContextManager, ExitStack, asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from copy import copy
from typing import (
    Annotated,
    Any,
    Literal,
    TypeVar,
    cast,
    overload,
)

from fastapi import Depends, FastAPI, Request, Response
from fastapi.dependencies.utils import (
    get_body_field,
    get_parameterless_sub_dependant,
    lenient_issubclass,
)
from fastapi.routing import APIRoute, APIRouter
from pydantic import BaseModel

from fastapi_pagination.pydantic.consts import IS_PYDANTIC_V2_12_5_OR_HIGHER
from fastapi_pagination.typing_utils import create_annotated_tp

from .pydantic.v2 import FieldV2, UndefinedV2

try:
    from fastapi.routing import request_response
except ImportError:  # pragma: no cover
    from starlette.routing import request_response

from .bases import AbstractPage, AbstractParams, BaseAbstractPage
from .errors import UninitializedConfigurationError
from .pydantic import IS_PYDANTIC_V2
from .types import AsyncItemsTransformer, ItemsTransformer, SyncItemsTransformer
from .utils import is_async_callable, unwrap_annotated

T = TypeVar("T")
TAbstractParams_co = TypeVar("TAbstractParams_co", covariant=True, bound=AbstractParams)

_params_val: ContextVar[AbstractParams] = ContextVar("_params_val")
_page_val: ContextVar[type[AbstractPage[Any]]] = ContextVar("_page_val")

_rsp_val: ContextVar[Response] = ContextVar("_rsp_val")
_req_val: ContextVar[Request] = ContextVar("_req_val")

_items_val: ContextVar[Sequence[Any]] = ContextVar("_items_val")
_items_transformer_val: ContextVar[ItemsTransformer | None] = ContextVar("_items_transformer_val", default=None)


def resolve_params(params: TAbstractParams_co | None = None) -> TAbstractParams_co:
    if params is None:
        try:
            return cast(TAbstractParams_co, _params_val.get())
        except LookupError:
            raise UninitializedConfigurationError("Use params, add_pagination or pagination_ctx") from None

    return params


def resolve_items_transformer(transformer: ItemsTransformer | None = None) -> ItemsTransformer | None:
    if transformer is None:
        return _items_transformer_val.get()

    return transformer


def pagination_items() -> Sequence[Any]:
    try:
        return _items_val.get()
    except LookupError:
        raise UninitializedConfigurationError("pagination_items must be called inside create_page") from None


def create_page(
    items: Sequence[T],
    /,
    total: int | None = None,
    params: AbstractParams | None = None,
    **kwargs: Any,
) -> AbstractPage[T]:
    """
    Creates an instance of AbstractPage with provided items and optional parameters.

    Returns:
        AbstractPage[T]: An instance of AbstractPage containing the provided items and additional parameters.
    """
    if total is not None:
        kwargs["total"] = total
    if params is not None:
        kwargs["params"] = params

    with _ctx_var_with_reset(_items_val, items):
        return resolve_page(params).create(items, **kwargs)


def response() -> Response:
    try:
        return _rsp_val.get()
    except LookupError:
        raise RuntimeError("response context var must be set") from None


def request() -> Request:
    try:
        return _req_val.get()
    except LookupError:
        raise RuntimeError("request context var must be set") from None


def _ctx_var_with_reset(var: ContextVar[T], value: T) -> AbstractContextManager[None]:
    token = var.set(value)

    @contextmanager
    def _reset_ctx() -> Iterator[None]:
        yield

        with suppress(ValueError):
            var.reset(token)

    return _reset_ctx()


def set_params(params: AbstractParams) -> AbstractContextManager[None]:
    return _ctx_var_with_reset(_params_val, params)


def set_page(page: type[AbstractPage[Any]]) -> AbstractContextManager[None]:
    return _ctx_var_with_reset(_page_val, page)


def resolve_page(params: AbstractParams | None = None, /) -> type[AbstractPage[Any]]:
    try:
        return _page_val.get()
    except LookupError:
        if params and (page := params.__page_type__):
            return page

        raise UninitializedConfigurationError(
            "can't resolve page type, use set_page or pagination_ctx with page argument, or use "
            "params that connected to page via set_page method"
        ) from None


def set_items_transformer(transformer: ItemsTransformer) -> AbstractContextManager[None]:
    return _ctx_var_with_reset(_items_transformer_val, transformer)


async def async_wrapped(obj: T) -> T:
    return obj


@overload
def apply_items_transformer(
    items: Sequence[Any],
    /,
    transformer: SyncItemsTransformer | None = None,
    *,
    async_: Literal[False] = False,
) -> Sequence[Any]:
    pass


@overload
async def apply_items_transformer(
    items: Sequence[Any],
    /,
    transformer: AsyncItemsTransformer | None = None,
    *,
    async_: Literal[True],
) -> Sequence[Any]:
    pass


def apply_items_transformer(
    items: Sequence[Any],
    /,
    transformer: ItemsTransformer | None = None,
    *,
    async_: bool = False,
) -> Any:
    transformer = resolve_items_transformer(transformer)

    if transformer is None:
        return async_wrapped(items) if async_ else items

    is_coro = is_async_callable(transformer)

    if is_coro and not async_:
        raise ValueError("apply_items_transformer called with async_=False but transformer is async")

    if is_coro:
        return transformer(items)

    items = transformer(items)  # type: ignore[assignment]
    return async_wrapped(items) if async_ else items


@cache
def _model_validate_has_by_name_param() -> bool:
    sign = inspect.signature(BaseModel.model_validate)
    return "by_name" in sign.parameters


def _create_params_dependency(  # noqa: C901
    params: type[TAbstractParams_co],
) -> Callable[[TAbstractParams_co], AsyncIterator[TAbstractParams_co]]:
    is_pydantic_v2_model = False

    async def _pagination_params(*args: Any, **kwargs: Any) -> AsyncIterator[TAbstractParams_co]:
        if is_pydantic_v2_model and _model_validate_has_by_name_param():
            # should not happen cause all , but let's be safe
            if args:  # pragma: no cover
                raise ValueError("Positional arguments are not supported for Pydantic v2 models")

            val = cast(
                AbstractParams,
                cast(type[BaseModel], params).model_validate(kwargs, by_name=True),
            )
        else:
            val = params(*args, **kwargs)

        with set_params(val):
            yield val

    sign = inspect.signature(params)

    if IS_PYDANTIC_V2:
        if IS_PYDANTIC_V2_12_5_OR_HIGHER:

            def _get_param(name: str, field: FieldV2) -> inspect.Parameter:
                field = copy(field)

                param_default: Any
                if field.default is not UndefinedV2:
                    # for pydantic v2.12.5+ we need to move default value to be as parameter default
                    param_default = field.default
                    field.default = UndefinedV2
                else:
                    param_default = inspect.Parameter.empty

                return inspect.Parameter(
                    name=name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=create_annotated_tp(field.annotation, field),
                    default=param_default,
                )
        else:

            def _get_param(name: str, field: FieldV2) -> inspect.Parameter:
                return inspect.Parameter(
                    name=name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=field.annotation,
                    default=field,
                )

        with suppress(ValueError, TypeError):
            is_pydantic_v2_model = issubclass(params, BaseModel)

        if is_pydantic_v2_model:
            sign_params = [_get_param(name, field) for name, field in cast(BaseModel, params).model_fields.items()]
            sign = sign.replace(parameters=sign_params)

    _pagination_params.__signature__ = sign  # type: ignore[attr-defined]

    return _pagination_params


async def _noop_dep() -> None:
    pass


def pagination_ctx(
    page: type[AbstractPage[Any]] | None = None,
    params: type[AbstractParams] | None = None,
    transformer: ItemsTransformer | None = None,
    __page_ctx_dep__: bool = False,
) -> Callable[..., AsyncIterator[AbstractParams]]:
    if page is not None and params is None:
        params = page.__params_type__

    params_dep: Any = _create_params_dependency(params) if params is not None else _noop_dep

    async def _page_ctx_dependency(
        req: Request,
        res: Response,
        _params: Annotated[Any, Depends(params_dep)],
    ) -> AsyncIterator[AbstractParams]:
        with ExitStack() as stack:
            if page is not None:
                stack.enter_context(set_page(page))
            if transformer is not None:
                stack.enter_context(set_items_transformer(transformer))

            stack.enter_context(_ctx_var_with_reset(_rsp_val, res))
            stack.enter_context(_ctx_var_with_reset(_req_val, req))

            yield cast(AbstractParams, _params)

    if __page_ctx_dep__:
        _page_ctx_dependency.__page_ctx_dep__ = True  # type: ignore[attr-defined]

    return _page_ctx_dependency


def _bet_body_field(route: APIRoute) -> Any | None:
    try:
        # starting from fastapi 0.113.0 get_body_field changed its signature
        return get_body_field(
            flat_dependant=route.dependant,
            name=route.unique_id,
            embed_body_fields=route._embed_body_fields,
        )
    except (TypeError, AttributeError):
        return get_body_field(  # type: ignore[missing-argument]
            dependant=route.dependant,  # type: ignore[call-arg]
            name=route.unique_id,
        )


ParentT = TypeVar("ParentT", APIRouter, FastAPI)


def _update_route(route: APIRoute) -> None:
    if any(hasattr(d.call, "__page_ctx_dep__") for d in route.dependant.dependencies):
        return

    page_cls = unwrap_annotated(route.response_model)

    if not lenient_issubclass(page_cls, BaseAbstractPage):
        return

    cls = cast(type[AbstractPage[Any]], page_cls)
    dep = Depends(pagination_ctx(cls, __page_ctx_dep__=True))

    route.dependencies.append(dep)
    route.dependant.dependencies.append(
        get_parameterless_sub_dependant(
            depends=dep,
            path=route.path_format,
        ),
    )

    route.body_field = _bet_body_field(route)
    route.app = request_response(route.get_route_handler())


def _patch_openapi(dst: dict[str, Any], src: dict[str, Any]) -> None:
    with suppress(KeyError):
        dst["paths"].update(src["paths"])

    with suppress(KeyError):
        dst["components"]["schemas"].update(src["components"]["schemas"])


def _add_pagination(parent: FastAPI | APIRouter, /) -> None:
    for route in parent.routes:
        # Avoid starlette routes (autogenerated for documentation)
        if isinstance(route, APIRoute):
            _update_route(route)

    # Patch OpenAPI schema if it's already generated
    if isinstance(parent, FastAPI) and parent.openapi_schema:
        old_schema, parent.openapi_schema = parent.openapi_schema, None
        _patch_openapi(old_schema, parent.openapi())
        parent.openapi_schema = old_schema


def add_pagination(parent: ParentT) -> ParentT:
    _add_pagination(parent)

    router = parent.router if isinstance(parent, FastAPI) else parent
    _original_lifespan_context = router.lifespan_context

    @asynccontextmanager
    async def lifespan(app: Any) -> AsyncIterator[Any]:
        _add_pagination(parent)

        async with _original_lifespan_context(app) as maybe_state:
            yield maybe_state

    router.lifespan_context = lifespan
    return parent
