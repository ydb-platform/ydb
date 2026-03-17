__all__ = [
    "apaginate",
    "apaginate_aggregate",
    "paginate",
    "paginate_aggregate",
]

from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from motor.core import AgnosticCollection
from typing_extensions import deprecated

from fastapi_pagination.api import apply_items_transformer, create_page
from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.ext.mongo import AggrPipelineTransformer
from fastapi_pagination.ext.utils import get_mongo_pipeline_filter_end
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer
from fastapi_pagination.utils import verify_params

if TYPE_CHECKING:
    _AgnosticCollection: TypeAlias = AgnosticCollection[Any]
else:
    _AgnosticCollection = AgnosticCollection


# TODO: refactor this function using flows
@deprecated("Motor will be deprecated on May 14, 2026. Migration to the PyMongo Async driver is recommended.")
async def apaginate(
    collection: _AgnosticCollection,
    query_filter: dict[Any, Any] | None = None,
    params: AbstractParams | None = None,
    sort: Any | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    **kwargs: Any,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset")
    query_filter = query_filter or {}

    total = await collection.count_documents(query_filter) if raw_params.include_total else None
    cursor = collection.find(query_filter, skip=raw_params.offset, limit=raw_params.limit, **kwargs)
    if sort is not None:
        cursor = cursor.sort(*sort) if isinstance(sort, tuple) else cursor.sort(sort)

    items = await cursor.to_list(length=raw_params.limit)
    t_items = await apply_items_transformer(items, transformer, async_=True)

    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )


@deprecated("Motor will be deprecated on May 14, 2026. Migration to the PyMongo Async driver is recommended.")
async def apaginate_aggregate(
    collection: _AgnosticCollection,
    aggregate_pipeline: list[dict[Any, Any]] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    aggregation_filter_end: int | Literal["auto"] | None = None,
    aggregation_pipeline_transformer: AggrPipelineTransformer | None = None,
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

    cursor = collection.aggregate(pipeline)

    data, *_ = await cursor.to_list(length=None)

    items = data["data"]
    try:
        total = data["metadata"][0]["total"]
    except IndexError:
        total = 0

    t_items = await apply_items_transformer(items, transformer, async_=True)

    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )


@deprecated("Use `apaginate` instead. This function will be removed in v0.16.0")
async def paginate(
    collection: _AgnosticCollection,
    query_filter: dict[Any, Any] | None = None,
    params: AbstractParams | None = None,
    sort: Any | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    **kwargs: Any,
) -> Any:
    return await apaginate(
        collection=collection,
        query_filter=query_filter,
        params=params,
        sort=sort,
        transformer=transformer,
        additional_data=additional_data,
        **kwargs,
    )


@deprecated("Use `apaginate_aggregate` instead. This function will be removed in v0.16.0")
async def paginate_aggregate(
    collection: _AgnosticCollection,
    aggregate_pipeline: list[dict[Any, Any]] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
) -> Any:
    return await apaginate_aggregate(
        collection=collection,
        aggregate_pipeline=aggregate_pipeline,
        params=params,
        transformer=transformer,
        additional_data=additional_data,
    )
