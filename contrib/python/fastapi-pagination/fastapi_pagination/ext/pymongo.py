from __future__ import annotations

__all__ = [
    "apaginate",
    "apaginate_aggregate",
    "paginate",
    "paginate_aggregate",
]


from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.collection import Collection

from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.config import Config
from fastapi_pagination.ext.mongo import AggrPipelineTransformer
from fastapi_pagination.ext.utils import get_mongo_pipeline_filter_end
from fastapi_pagination.flow import flow, flow_expr, run_async_flow, run_sync_flow
from fastapi_pagination.flows import create_page_flow, generic_flow
from fastapi_pagination.types import AdditionalData, ItemsTransformer, SyncItemsTransformer
from fastapi_pagination.utils import verify_params

T = TypeVar("T", bound=Mapping[str, Any])


def paginate(
    collection: Collection[T],
    query_filter: dict[Any, Any] | None = None,
    filter_fields: dict[Any, Any] | None = None,
    params: AbstractParams | None = None,
    sort: Sequence[Any] | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
    **kwargs: Any,
) -> Any:
    query_filter = query_filter or {}

    return run_sync_flow(
        generic_flow(
            total_flow=flow_expr(lambda: collection.count_documents(query_filter)),
            limit_offset_flow=flow_expr(
                lambda raw_params: collection.find(
                    query_filter,
                    filter_fields,
                    skip=raw_params.offset,
                    limit=raw_params.limit,
                    sort=sort,
                    **kwargs,
                ).to_list()
            ),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )


async def apaginate(
    collection: AsyncCollection[T],
    query_filter: dict[Any, Any] | None = None,
    filter_fields: dict[Any, Any] | None = None,
    params: AbstractParams | None = None,
    sort: Sequence[Any] | None = None,
    *,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
    **kwargs: Any,
) -> Any:
    query_filter = query_filter or {}

    return await run_async_flow(
        generic_flow(
            total_flow=flow_expr(lambda: collection.count_documents(query_filter)),
            limit_offset_flow=flow_expr(
                lambda raw_params: collection.find(
                    query_filter,
                    filter_fields,
                    skip=raw_params.offset,
                    limit=raw_params.limit,
                    sort=sort,
                    **kwargs,
                ).to_list()
            ),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )


@flow
def _aggregate_flow(
    is_async: bool,
    collection: Collection[T] | AsyncCollection[T],
    aggregate_pipeline: list[dict[Any, Any]] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    aggregation_filter_end: int | Literal["auto"] | None = None,
    aggregation_pipeline_transformer: AggrPipelineTransformer | None = None,
    config: Config | None = None,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset")
    aggregate_pipeline = aggregate_pipeline or []

    paginate_data = []
    if raw_params.limit is not None:
        paginate_data.append({"$limit": raw_params.limit + (raw_params.offset or 0)})
    if raw_params.offset is not None:
        paginate_data.append({"$skip": raw_params.offset})

    if aggregation_filter_end is not None:
        if aggregation_filter_end == "auto":
            aggregation_filter_end = get_mongo_pipeline_filter_end(aggregate_pipeline)
        transform_part = aggregate_pipeline[:aggregation_filter_end]
        aggregate_pipeline = aggregate_pipeline[aggregation_filter_end:]
        paginate_data.extend(transform_part)

    pipeline = [
        *aggregate_pipeline,
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": paginate_data,
            },
        },
    ]

    if aggregation_pipeline_transformer is not None:
        pipeline = aggregation_pipeline_transformer(pipeline)

    cursor = yield collection.aggregate(pipeline)

    data, *_ = yield cursor.to_list(length=None)

    items = data["data"]
    try:
        total = data["metadata"][0]["total"]
    except IndexError:
        total = 0

    page = yield from create_page_flow(
        items,
        params,
        total=total,
        transformer=transformer,
        additional_data=additional_data,
        config=config,
        async_=is_async,
    )

    return page


async def apaginate_aggregate(
    collection: AsyncCollection[T],
    aggregate_pipeline: list[dict[Any, Any]] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    aggregation_filter_end: int | Literal["auto"] | None = None,
    aggregation_pipeline_transformer: AggrPipelineTransformer | None = None,
    config: Config | None = None,
) -> Any:
    return await run_async_flow(
        _aggregate_flow(
            is_async=True,
            collection=collection,
            aggregate_pipeline=aggregate_pipeline or [],
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            aggregation_filter_end=aggregation_filter_end,
            aggregation_pipeline_transformer=aggregation_pipeline_transformer,
            config=config,
        )
    )


def paginate_aggregate(
    collection: Collection[T],
    aggregate_pipeline: list[dict[Any, Any]] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    aggregation_filter_end: int | Literal["auto"] | None = None,
    aggregation_pipeline_transformer: AggrPipelineTransformer | None = None,
    config: Config | None = None,
) -> Any:
    return run_sync_flow(
        _aggregate_flow(
            is_async=False,
            collection=collection,
            aggregate_pipeline=aggregate_pipeline or [],
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            aggregation_filter_end=aggregation_filter_end,
            aggregation_pipeline_transformer=aggregation_pipeline_transformer,
            config=config,
        )
    )
