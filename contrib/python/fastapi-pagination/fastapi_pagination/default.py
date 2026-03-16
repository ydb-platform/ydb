from __future__ import annotations

__all__ = [
    "Page",
    "Params",
]

from collections.abc import Sequence
from math import ceil
from typing import Any, Generic

from fastapi import Query
from pydantic import BaseModel
from typing_extensions import TypeVar

from .bases import AbstractParams, BasePage, RawParams
from .pydantic import create_pydantic_model
from .types import GreaterEqualOne, GreaterEqualZero

TAny = TypeVar("TAny", default=Any)


class Params(BaseModel, AbstractParams):
    page: int = Query(1, ge=1, description="Page number")
    size: int = Query(50, ge=1, le=100, description="Page size")

    def to_raw_params(self) -> RawParams:
        return RawParams(
            limit=self.size if self.size is not None else None,
            offset=self.size * (self.page - 1) if self.page is not None and self.size is not None else None,
        )


class Page(BasePage[TAny], Generic[TAny]):
    page: GreaterEqualOne
    size: GreaterEqualOne
    pages: GreaterEqualZero

    __params_type__ = Params

    @classmethod
    def create(
        cls,
        items: Sequence[TAny],
        params: AbstractParams,
        *,
        total: int | None = None,
        **kwargs: Any,
    ) -> Page[TAny]:
        if not isinstance(params, Params):
            raise TypeError("Page should be used with Params")

        size = params.size if params.size is not None else (total or None)
        page = params.page if params.page is not None else 1

        if size in {0, None}:
            pages = 0
        elif total is not None:
            pages = ceil(total / size)
        else:
            pages = None

        return create_pydantic_model(
            cls,
            total=total,
            items=items,
            page=page,
            size=size,
            pages=pages,
            **kwargs,
        )
