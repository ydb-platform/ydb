__all__ = ["paginate"]

from typing import Any, TypeVar, cast

from django.db.models import Model, QuerySet
from django.db.models.base import ModelBase

from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.config import Config
from fastapi_pagination.flow import flow_expr, run_sync_flow
from fastapi_pagination.flows import generic_flow
from fastapi_pagination.types import AdditionalData, SyncItemsTransformer

T = TypeVar("T", bound=Model)


def paginate(
    query: type[T] | QuerySet[T],
    params: AbstractParams | None = None,
    *,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    config: Config | None = None,
) -> Any:
    if isinstance(query, ModelBase):
        query = query.objects.all()  # type: ignore[possibly-missing-attribute]

    query_set = cast(QuerySet[T], query)

    return run_sync_flow(
        generic_flow(
            total_flow=flow_expr(lambda: query_set.count()),
            limit_offset_flow=flow_expr(lambda raw_params: [*query_set[raw_params.as_slice()]]),
            params=params,
            transformer=transformer,
            additional_data=additional_data,
            config=config,
        )
    )
