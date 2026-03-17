__all__ = ["apaginate", "paginate"]

from typing import Any

from orm.models import QuerySet
from typing_extensions import deprecated

from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.config import Config
from fastapi_pagination.ext.utils import generic_query_apply_params
from fastapi_pagination.flow import flow_expr, run_async_flow
from fastapi_pagination.flows import generic_flow
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer


@deprecated("`orm` project is not longer maintained and this extension will be removed in v0.16.0")
async def apaginate(
    query: QuerySet,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return await run_async_flow(
        generic_flow(
            async_=True,
            total_flow=flow_expr(lambda: query.count()),
            limit_offset_flow=flow_expr(lambda raw_params: generic_query_apply_params(query, raw_params).all()),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )


@deprecated("`orm` project is not longer maintained and this extension will be removed in v0.16.0")
async def paginate(
    query: QuerySet,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return await apaginate(  # type: ignore[deprecated]
        query,
        params=params,
        transformer=transformer,
        additional_data=additional_data,
        config=config,
    )
