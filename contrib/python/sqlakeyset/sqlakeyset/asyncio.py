from __future__ import annotations

from typing import Any, Optional, Tuple, TypeVar, Union

from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.sql.selectable import Select

from sqlakeyset.paging import (
    PER_PAGE_DEFAULT,
    OptionalKeyset,
    core_page_from_rows,
    prepare_paging,
    process_args,
)
from sqlakeyset.results import Page
from sqlakeyset.sqla import core_result_type, get_bind
from sqlakeyset.types import Keyset, MarkerLike

_TP = TypeVar("_TP", bound=Tuple[Any, ...])


async def core_get_page(
    s: Union[AsyncSession, AsyncConnection],
    selectable: Select[_TP],
    per_page: int,
    place: Optional[Keyset],
    backwards: bool,
    unique: bool,
) -> Page[Row[_TP]]:
    result_type = core_result_type(selectable, s)
    sel = prepare_paging(
        q=selectable,
        per_page=per_page,
        place=place,
        backwards=backwards,
        orm=False,
        dialect=get_bind(q=selectable, s=s).dialect,  # type: ignore
    )
    selected = await s.execute(sel.select)
    keys = list(selected.keys())
    N = len(keys) - len(sel.extra_columns)
    keys = keys[:N]
    if unique:
        selected = selected.unique()
    page = core_page_from_rows(
        sel,
        selected.fetchall(),
        keys,
        result_type,
        per_page,
        backwards,
        current_place=place,
    )
    return page


async def select_page(
    s: Union[AsyncSession, AsyncConnection],
    selectable: Select[_TP],
    per_page: int = PER_PAGE_DEFAULT,
    unique: bool = False,
    after: OptionalKeyset = None,
    before: OptionalKeyset = None,
    page: Optional[Union[MarkerLike, str]] = None,
) -> Page[Row[_TP]]:
    """Get a page of results from a SQLAlchemy Core (or new-style ORM)
    selectable using an asyncio connection.

    Specify no more than one of the arguments ``page``, ``after`` or
    ``before``. If none of these are provided, the first page is returned.

    :param s: :class:`sqlalchemy.engine.Connection` or
        :class:`sqlalchemy.orm.session.Session` to use to execute the query.
    :param selectable: The source selectable.
    :param per_page: The (maximum) number of rows on the page.
    :param unique: whether to return only unique rows.
    :type per_page: int, optional.
    :param page: a ``(keyset, backwards)`` pair or string bookmark describing
        the page to get.
    :param after: if provided, the page will consist of the rows immediately
        following the specified keyset.
    :param before: if provided, the page will consist of the rows immediately
        preceding the specified keyset.

    :returns: A :class:`Page` containing the requested rows and paging hooks
        to access surrounding pages.
    """
    place, backwards = process_args(after, before, page)
    return await core_get_page(s, selectable, per_page, place, backwards, unique=unique)
