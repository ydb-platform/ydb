from __future__ import annotations

__all__ = [
    "create_count_query_from_text",
    "create_paginate_query_from_text",
]

from typing import TypeAlias

from fastapi_pagination.bases import AbstractParams, RawParams

AnyParams: TypeAlias = AbstractParams | RawParams


def _unwrap_params(params: AnyParams) -> RawParams:
    if isinstance(params, RawParams):
        return params

    return params.to_raw_params().as_limit_offset()


def create_paginate_query_from_text(query: str, params: AnyParams) -> str:
    raw_params = _unwrap_params(params)

    suffix = ""
    if raw_params.limit is not None:
        suffix += f" LIMIT {raw_params.limit}"
    if raw_params.offset is not None:
        suffix += f" OFFSET {raw_params.offset}"

    return f"{query} {suffix}".strip()


def create_count_query_from_text(query: str) -> str:
    return f"SELECT count(*) FROM ({query}) AS __count_query__"  # noqa: S608
