__all__ = ["paginate"]

from collections.abc import Mapping
from typing import Any, TypeVar

from cassandra.cluster import SimpleStatement
from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model

from fastapi_pagination.api import apply_items_transformer, create_page
from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.types import AdditionalData, SyncItemsTransformer
from fastapi_pagination.utils import verify_params

T = TypeVar("T", bound=Mapping[str, Any])


def paginate(
    model: type[Model],
    query_filter: dict[Any, Any] | None = None,
    params: AbstractParams | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
) -> Any:
    params, raw_params = verify_params(params, "cursor")
    assert not raw_params.include_total, "Cassandra does not support total count"

    query_filter = query_filter or {}
    stmt = SimpleStatement(
        str(model.filter(**query_filter)),
        fetch_size=raw_params.size,
    )
    conn = connection.get_connection()

    cursor = conn.session.execute(
        stmt,
        parameters={str(i): v for i, v in enumerate(query_filter.values())},
        paging_state=raw_params.cursor,
    )
    items = cursor.current_rows
    t_items = apply_items_transformer(items, transformer)

    return create_page(
        t_items,
        params=params,
        next_=cursor.paging_state,
        **(additional_data or {}),
    )
