__all__ = [
    "paginate",
]

from functools import partial
from typing import Any, cast

from elasticsearch import Elasticsearch
from elasticsearch.dsl import Search

from fastapi_pagination.bases import AbstractParams, CursorRawParams
from fastapi_pagination.config import Config
from fastapi_pagination.flow import flow, flow_expr, run_sync_flow
from fastapi_pagination.flows import CursorFlow, generic_flow
from fastapi_pagination.types import AdditionalData, SyncItemsTransformer


@flow
def _cursor_flow(
    query: Search,
    conn: Elasticsearch,
    raw_params: CursorRawParams,
) -> CursorFlow:
    response: Any
    if not raw_params.cursor:
        response = yield query.params(scroll="1m").extra(size=raw_params.size).execute()
        items = response.hits
        next_ = response._scroll_id
    else:
        response = yield conn.scroll(scroll_id=cast(str, raw_params.cursor), scroll="1m")
        next_ = response.get("_scroll_id")
        items = [item.get("_source") for item in response["hits"]["hits"]]

    return items, {"next_": next_}


def paginate(
    conn: Elasticsearch,
    query: Search,
    params: AbstractParams | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    return run_sync_flow(
        generic_flow(
            total_flow=flow_expr(lambda: query.using(conn).count()),
            limit_offset_flow=flow_expr(lambda raw_params: query.using(conn)[raw_params.as_slice()].execute()),
            cursor_flow=partial(_cursor_flow, query, conn),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )
