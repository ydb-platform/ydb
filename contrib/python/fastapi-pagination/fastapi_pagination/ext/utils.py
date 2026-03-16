__all__ = [
    "generic_query_apply_params",
    "get_mongo_pipeline_filter_end",
    "len_or_none",
    "unwrap_scalars",
    "wrap_scalars",
]

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar, cast

from typing_extensions import Self

from fastapi_pagination.bases import RawParams

T = TypeVar("T")


def len_or_none(obj: Any) -> int | None:
    try:
        return len(obj)
    except TypeError:
        return None


def unwrap_scalars(
    items: Sequence[Sequence[T]],
    *,
    force_unwrap: bool = False,
) -> Sequence[T] | Sequence[Sequence[T]]:
    return cast(
        Sequence[T] | Sequence[Sequence[T]],
        [item[0] if force_unwrap or len_or_none(item) == 1 else item for item in items],
    )


def wrap_scalars(items: Sequence[Any]) -> Sequence[Sequence[Any]]:
    return [item if len_or_none(item) is not None else [item] for item in items]


class AbstractQuery(Protocol):
    def limit(self, *_: Any, **__: Any) -> Self:  # pragma: no cover
        pass

    def offset(self, *_: Any, **__: Any) -> Self:  # pragma: no cover
        pass


TAbstractQuery = TypeVar("TAbstractQuery", bound=AbstractQuery)


def generic_query_apply_params(q: TAbstractQuery, params: RawParams) -> TAbstractQuery:
    if params.limit is not None:
        q = q.limit(params.limit)
    if params.offset is not None:
        q = q.offset(params.offset)

    return q


def get_mongo_pipeline_filter_end(
    aggregate_pipeline: list[dict[str, Any]],
) -> int:
    """
    Get the index of the stage in the aggregation pipeline where the number or order
    of documents in the pipeline no longer changes.
    """

    # MongoDB aggregation pipeline stages that do not change the number or order
    # of documents in the pipeline output.
    transform_stages = [
        "$addFields",
        "$graphLookup",
        "$lookup",
        "$project",
        "$replaceRoot",
        "$replaceWith",
        "$set",
        "$unset",
    ]
    for i, stage in enumerate(reversed(aggregate_pipeline)):
        if any(stage_name not in transform_stages for stage_name in stage):
            return len(aggregate_pipeline) - i
    return 0
