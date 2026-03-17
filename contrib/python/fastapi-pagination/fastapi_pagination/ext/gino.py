from __future__ import annotations

__all__ = ["apaginate", "paginate"]

from typing import Any

from gino.crud import CRUDModel
from sqlalchemy import func, literal_column
from sqlalchemy.sql import Select
from typing_extensions import deprecated

from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.config import Config
from fastapi_pagination.flow import flow_expr, run_async_flow
from fastapi_pagination.flows import generic_flow
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer

from .sqlalchemy import create_paginate_query


@deprecated("`gino` project is not longer maintained and this extension will be removed in v0.16.0")
async def apaginate(
    query: Select[tuple[Any, ...]] | CRUDModel,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    if isinstance(query, type) and issubclass(query, CRUDModel):
        query = query.query  # type: ignore[attr-defined]

    return await run_async_flow(
        generic_flow(
            total_flow=flow_expr(
                lambda: (
                    func.count(literal_column("*"))
                    .select()
                    .select_from(
                        query.order_by(None).alias(),
                    )
                    .gino.scalar()
                )  # type: ignore[attr-defined]
            ),
            limit_offset_flow=flow_expr(
                lambda raw_params: create_paginate_query(query, raw_params).gino.all()  # type: ignore[union-attr]
            ),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
            async_=True,
        )
    )


@deprecated("`gino` project is not longer maintained and this extension will be removed in v0.16.0")
async def paginate(
    query: Select[tuple[Any, ...]] | CRUDModel,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return await apaginate(
        query,
        params=params,
        transformer=transformer,
        additional_data=additional_data,
        config=config,
    )
