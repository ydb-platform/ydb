from __future__ import annotations

__all__ = ["apaginate", "paginate"]

import inspect
from copy import copy
from typing import Any, Literal, TypeVar

from beanie import Document, PydanticObjectId
from beanie.odm.enums import SortDirection
from beanie.odm.interfaces.aggregate import DocumentProjectionType
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.find import FindMany
from beanie.odm.utils.projection import get_projection
from bson.errors import InvalidId
from pymongo.asynchronous.client_session import AsyncClientSession
from typing_extensions import deprecated

from fastapi_pagination.api import apply_items_transformer, create_page
from fastapi_pagination.bases import AbstractParams, is_cursor, is_limit_offset
from fastapi_pagination.ext.mongo import AggrPipelineTransformer
from fastapi_pagination.ext.utils import get_mongo_pipeline_filter_end
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer
from fastapi_pagination.utils import verify_params

TDocument = TypeVar("TDocument", bound=Document)


def parse_cursor(cursor: str) -> PydanticObjectId:
    try:
        return PydanticObjectId(cursor.split("_", 1)[-1])
    except InvalidId as exc:
        raise ValueError("Invalid cursor") from exc


# TODO: simplify this function using flows
# TODO: refactor it before 0.16.0 release
async def apaginate(  # noqa: C901, PLR0912, PLR0915
    query: TDocument | FindMany[TDocument] | AggregationQuery[TDocument],
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    projection_model: type[DocumentProjectionType] | None = None,
    sort: None | str | list[tuple[str, SortDirection]] = None,
    session: AsyncClientSession | None = None,
    ignore_cache: bool = False,
    fetch_links: bool = False,
    lazy_parse: bool = False,
    aggregation_filter_end: int | Literal["auto"] | None = None,
    aggregation_pipeline_transformer: AggrPipelineTransformer | None = None,
    **pymongo_kwargs: Any,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset", "cursor")
    if additional_data is None:
        additional_data = {}

    cursor = getattr(raw_params, "cursor", None)
    if isinstance(query, AggregationQuery):
        aggregation_query = query.clone()

        # Get the projection from the query's projection_model if it exists.
        # We need to include it inside the $facet's data pipeline because Beanie
        # appends $project at the end of the pipeline (after $facet), which breaks
        # the aggregation since $facet changes the document structure.
        # See: https://github.com/uriyyo/fastapi-pagination/issues/1514
        projection_pipeline: list[dict[str, Any]] = []
        if aggregation_query.projection_model is not None:
            projection = get_projection(aggregation_query.projection_model)
            if projection is not None:
                projection_pipeline = [{"$project": projection}]
            # Clear the projection_model so Beanie doesn't append $project after $facet
            aggregation_query.projection_model = None

        paginate_data: list[dict[str, Any]] = []
        if is_limit_offset(raw_params):
            if raw_params.limit is not None:
                paginate_data.append({"$limit": raw_params.limit + (raw_params.offset or 0)})
            if raw_params.offset is not None:
                paginate_data.append({"$skip": raw_params.offset})
        elif cursor:
            if cursor.startswith("prev_"):
                paginate_data.append(
                    {
                        "_id": {
                            "$lt": parse_cursor(cursor),
                        },
                    },
                )
            else:
                paginate_data.append(
                    {
                        "_id": {
                            "$gt": parse_cursor(cursor),
                        },
                    },
                )
        if aggregation_filter_end is not None:
            if aggregation_filter_end == "auto":
                aggregation_filter_end = get_mongo_pipeline_filter_end(aggregation_query.aggregation_pipeline)
            filter_part = aggregation_query.aggregation_pipeline[:aggregation_filter_end]
            transform_part = aggregation_query.aggregation_pipeline[aggregation_filter_end:]
            aggregation_query.aggregation_pipeline = [
                *filter_part,
                {
                    "$facet": {
                        "metadata": [{"$count": "total"}],
                        "data": [*paginate_data, *transform_part, *projection_pipeline],
                    }
                },
            ]
        else:
            aggregation_query.aggregation_pipeline.extend(
                [
                    {"$facet": {"metadata": [{"$count": "total"}], "data": [*paginate_data, *projection_pipeline]}},
                ],
            )

        # Execute the aggregation pipeline directly using the underlying collection.
        # We bypass Beanie's to_list() because we've already handled the projection
        # and need to avoid Beanie appending $project after our $facet stage.
        pipeline = aggregation_query.get_aggregation_pipeline()

        if aggregation_pipeline_transformer is not None:
            pipeline = aggregation_pipeline_transformer(pipeline)

        mongo_cursor = aggregation_query.document_model.get_pymongo_collection().aggregate(
            pipeline,
            session=aggregation_query.session,
            **aggregation_query.pymongo_kwargs,
        )

        # in case of pymongo engine we need to await the cursor
        if inspect.iscoroutine(mongo_cursor):
            mongo_cursor = await mongo_cursor

        data = (await mongo_cursor.to_list(length=None))[0]
        items = data["data"]
        try:
            total = data["metadata"][0]["total"]
        except IndexError:
            total = 0
        if is_cursor(raw_params):
            if cursor and cursor.startswith("prev_"):
                items = list(reversed(items))
            additional_data["next_"] = str(items[-1].id) if items else None
            additional_data["previous"] = f"prev_{items[0].id}" if items else None
    else:
        # avoid original query mutation
        count_query = copy(query)
        query = copy(query)

        if raw_params.include_total:
            total = await count_query.find(
                {},
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                **pymongo_kwargs,
            ).count()
        else:
            total = None

        if is_limit_offset(raw_params):
            items = await query.find_many(
                limit=raw_params.limit,
                skip=raw_params.offset,
                projection_model=projection_model,
                sort=sort,
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                lazy_parse=lazy_parse,
                **pymongo_kwargs,
            ).to_list()
        else:
            query = query.find_many(
                projection_model=projection_model,
                sort=sort,
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                lazy_parse=lazy_parse,
                **pymongo_kwargs,
            )
            if cursor:
                if cursor.startswith("prev_"):
                    query = query.find(
                        {
                            "_id": {
                                "$lt": parse_cursor(cursor),
                            },
                        },
                    ).sort("-_id")
                else:
                    query = query.find(
                        {
                            "_id": {
                                "$gt": parse_cursor(cursor),
                            },
                        },
                    )

            items = await query.limit(raw_params.size + 1).to_list()  # type: ignore[attr-defined]
            next_link_available = items and len(items) >= raw_params.size  # type: ignore[attr-defined]
            items = items[: raw_params.size]  # type: ignore[attr-defined]
            if cursor and cursor.startswith("prev_"):
                items = list(reversed(items))
            additional_data["next_"] = str(items[-1].id) if next_link_available else None
            additional_data["previous"] = f"prev_{items[0].id}" if items else None

    t_items = await apply_items_transformer(items, transformer, async_=True)

    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )


@deprecated("Use `apaginate` instead. This function will be removed in v0.16.0")
async def paginate(
    query: TDocument | FindMany[TDocument] | AggregationQuery[TDocument],
    params: AbstractParams | None = None,
    *,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    projection_model: type[DocumentProjectionType] | None = None,
    sort: None | str | list[tuple[str, SortDirection]] = None,
    session: AsyncClientSession | None = None,
    ignore_cache: bool = False,
    fetch_links: bool = False,
    lazy_parse: bool = False,
    aggregation_filter_end: int | None = None,
    **pymongo_kwargs: Any,
) -> Any:
    return await apaginate(
        query,
        params=params,
        transformer=transformer,
        additional_data=additional_data,
        projection_model=projection_model,
        sort=sort,
        session=session,
        ignore_cache=ignore_cache,
        fetch_links=fetch_links,
        lazy_parse=lazy_parse,
        aggregation_filter_end=aggregation_filter_end,
        **pymongo_kwargs,
    )
