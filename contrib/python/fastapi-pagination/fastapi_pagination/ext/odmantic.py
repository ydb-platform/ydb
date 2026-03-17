__all__ = ["apaginate", "paginate"]

import warnings
from functools import partial
from typing import Any, TypeAlias, overload

from odmantic import AIOEngine, Model, SyncEngine
from odmantic.engine import AIOSessionType, SyncSessionType
from odmantic.query import QueryExpression
from typing_extensions import deprecated

from fastapi_pagination.bases import AbstractParams, RawParams
from fastapi_pagination.config import Config
from fastapi_pagination.flow import AnyFlow, flow, flow_expr, run_async_flow, run_sync_flow
from fastapi_pagination.flows import LimitOffsetFlow, generic_flow
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer, ItemsTransformer, SyncItemsTransformer

_Query: TypeAlias = QueryExpression | dict[Any, Any] | bool


@flow
def _limit_offset_flow(
    model: type[Model],
    queries: tuple[_Query, ...],
    sort: Any | None,
    engine: SyncEngine | AIOEngine,
    session: SyncSessionType | AIOSessionType | None,
    raw_params: RawParams,
) -> LimitOffsetFlow:
    result = yield engine.find(
        model,
        *queries,
        sort=sort,
        session=session,  # type: ignore[arg-type]
        limit=raw_params.limit,
        skip=raw_params.offset or 0,
    )

    return [*result]


@flow
def _paginate_flow(
    is_async: bool,
    engine: SyncEngine | AIOEngine,
    model: type[Model],
    *queries: _Query,
    # odmantic related
    sort: Any | None = None,
    session: SyncSessionType | AIOSessionType | None = None,
    # fastapi-pagination related
    params: AbstractParams | None = None,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> AnyFlow:
    page = yield from generic_flow(
        total_flow=flow_expr(
            lambda: engine.count(
                model,
                *queries,
                session=session,  # type: ignore[arg-type]
            ),
        ),
        limit_offset_flow=partial(
            _limit_offset_flow,
            model,
            queries,
            sort,
            engine,
            session,
        ),
        params=params,
        transformer=transformer,
        additional_data=additional_data,
        config=config,
        async_=is_async,
    )

    return page


@overload
@deprecated("Use `apaginate` instead. This function will be removed in v0.16.0")
async def paginate(
    engine: AIOEngine,
    model: type[Model],
    *queries: _Query,
    # odmantic related
    sort: Any | None = None,
    session: SyncSessionType | None = None,
    # fastapi-pagination related
    params: AbstractParams | None = None,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    pass


@overload
def paginate(
    engine: SyncEngine,
    model: type[Model],
    *queries: _Query,
    # odmantic related
    sort: Any | None = None,
    session: AIOSessionType | None = None,
    # fastapi-pagination related
    params: AbstractParams | None = None,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    pass


@deprecated("`odmantic` project is not longer maintained and this extension will be removed in v0.16.0")
def paginate(
    engine: SyncEngine | AIOEngine,
    model: type[Model],
    *queries: _Query,
    # odmantic related
    sort: Any | None = None,
    session: SyncSessionType | AIOSessionType | None = None,
    # fastapi-pagination related
    params: AbstractParams | None = None,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    if isinstance(engine, AIOEngine):
        warnings.warn(
            "Use `apaginate` instead. This function overload will be removed in v0.16.0",
            DeprecationWarning,
            stacklevel=2,
        )
        return apaginate(
            engine,
            model,
            *queries,
            sort=sort,
            session=session,  # type: ignore[arg-type]
            params=params,
            transformer=transformer,
            additional_data=additional_data,
        )

    return run_sync_flow(
        _paginate_flow(
            False,
            engine,
            model,
            *queries,
            sort=sort,
            session=session,
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )


async def apaginate(
    engine: AIOEngine,
    model: type[Model],
    *queries: _Query,
    # odmantic related
    sort: Any | None = None,
    session: SyncSessionType | None = None,
    # fastapi-pagination related
    params: AbstractParams | None = None,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return await run_async_flow(
        _paginate_flow(
            True,
            engine,
            model,
            *queries,
            sort=sort,
            session=session,
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )
