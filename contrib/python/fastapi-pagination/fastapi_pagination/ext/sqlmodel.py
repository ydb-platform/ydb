from __future__ import annotations

__all__ = ["apaginate", "paginate"]

import warnings
from typing import Any, Generic, TypeAlias, TypeVar, overload

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlmodel import Session, SQLModel, select
from sqlmodel.sql.expression import Select, SelectOfScalar
from typing_extensions import deprecated

from fastapi_pagination.bases import AbstractParams
from fastapi_pagination.config import Config
from fastapi_pagination.types import AdditionalData, AsyncItemsTransformer, ItemsTransformer, SyncItemsTransformer

from .sqlalchemy import apaginate as _apaginate
from .sqlalchemy import paginate as _paginate

try:
    from sqlmodel.sql._expression_select_cls import SelectBase
except ImportError:  # pragma: no cover
    _T = TypeVar("_T")

    class SelectBase(Generic[_T]):
        pass


T = TypeVar("T")
TSQLModel = TypeVar("TSQLModel", bound=SQLModel)


_InputQuery: TypeAlias = Select[TSQLModel] | type[TSQLModel] | SelectBase[TSQLModel] | SelectOfScalar[T]
_InputCountQuery: TypeAlias = Select[TSQLModel] | SelectOfScalar[T]


def _prepare_query(query: _InputQuery[TSQLModel, T], /) -> _InputQuery[TSQLModel, T]:
    if not isinstance(query, (Select, SelectOfScalar)):
        query = select(query)  # type: ignore[no-matching-overload]

    return query


@overload
def paginate(
    session: Session,
    query: _InputQuery[TSQLModel, T],
    params: AbstractParams | None = None,
    *,
    count_query: _InputCountQuery[TSQLModel, T] | None = None,
    subquery_count: bool = True,
    transformer: SyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    unique: bool = True,
    config: Config | None = None,
) -> Any:
    pass


@overload
@deprecated("Use `apaginate` instead. This function will be removed in v0.16.0")
async def paginate(
    session: AsyncSession | AsyncConnection,
    query: _InputQuery[TSQLModel, T],
    params: AbstractParams | None = None,
    *,
    count_query: _InputCountQuery[TSQLModel, T] | None = None,
    subquery_count: bool = True,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    unique: bool = True,
    config: Config | None = None,
) -> Any:
    pass


def paginate(
    session: AsyncSession | AsyncConnection | Session,
    query: Any,
    params: AbstractParams | None = None,
    *,
    count_query: Any | None = None,
    subquery_count: bool = True,
    transformer: ItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    unique: bool = True,
    config: Config | None = None,
) -> Any:
    query = _prepare_query(query)

    if count_query is not None:
        count_query = _prepare_query(count_query)

    if isinstance(session, (AsyncSession, AsyncConnection)):
        warnings.warn(
            "Use `apaginate` instead. This function overload will be removed in v0.16.0",
            DeprecationWarning,
            stacklevel=2,
        )

        return apaginate(
            session,
            query,
            params,
            count_query=count_query,  # type: ignore[invalid-argument-type]
            subquery_count=subquery_count,
            transformer=transformer,
            additional_data=additional_data,
            unique=unique,
            config=config,
        )

    return _paginate(
        session,
        query,
        params,
        count_query=count_query,
        subquery_count=subquery_count,
        transformer=transformer,
        additional_data=additional_data,
        unique=unique,
        config=config,
    )


async def apaginate(
    session: AsyncSession | AsyncConnection,
    query: _InputQuery[TSQLModel, T],
    params: AbstractParams | None = None,
    *,
    count_query: _InputCountQuery[TSQLModel, T] | None = None,
    subquery_count: bool = True,
    transformer: AsyncItemsTransformer | None = None,
    additional_data: AdditionalData | None = None,
    unique: bool = True,
    config: Config | None = None,
) -> Any:
    query = _prepare_query(query)

    if count_query is not None:
        count_query = _prepare_query(count_query)  # type: ignore[invalid-assignment]

    return await _apaginate(
        session,
        query,
        params,
        count_query=count_query,
        subquery_count=subquery_count,
        transformer=transformer,
        additional_data=additional_data,
        unique=unique,
        config=config,
    )
