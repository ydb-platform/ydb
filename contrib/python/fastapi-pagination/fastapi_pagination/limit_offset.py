from __future__ import annotations

__all__ = [
    "LimitOffsetPage",
    "LimitOffsetParams",
]

from collections.abc import Sequence
from typing import Any, Generic

from fastapi import Query
from pydantic import BaseModel
from typing_extensions import TypeVar

from .bases import AbstractParams, BasePage, RawParams
from .pydantic import create_pydantic_model
from .types import GreaterEqualOne, GreaterEqualZero

TAny = TypeVar("TAny", default=Any)


class LimitOffsetParams(BaseModel, AbstractParams):
    limit: int = Query(50, ge=1, le=100, description="Page size limit")
    offset: int = Query(0, ge=0, description="Page offset")

    def to_raw_params(self) -> RawParams:
        return RawParams(
            limit=self.limit,
            offset=self.offset,
        )


class LimitOffsetPage(BasePage[TAny], Generic[TAny]):
    limit: GreaterEqualOne
    offset: GreaterEqualZero

    __params_type__ = LimitOffsetParams

    @classmethod
    def create(
        cls,
        items: Sequence[TAny],
        params: AbstractParams,
        *,
        total: int | None = None,
        **kwargs: Any,
    ) -> LimitOffsetPage[TAny]:
        raw_params = params.to_raw_params().as_limit_offset()

        return create_pydantic_model(
            cls,
            total=total,
            items=items,
            limit=raw_params.limit,
            offset=raw_params.offset,
            **kwargs,
        )
