from __future__ import annotations

from typing import TYPE_CHECKING, Union

import sqlalchemy
from packaging import version
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm.scoping import ScopedSession

SQLA_VERSION = version.parse(sqlalchemy.__version__)


try:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
except ImportError:
    if not TYPE_CHECKING:

        class AsyncConnection:
            pass

        class AsyncEngine:
            pass

        class AsyncSession:
            pass


def get_bind(
    q, s: Union[Engine, Connection, Session, AsyncEngine, AsyncConnection, AsyncSession]
) -> Union[Engine, Connection]:
    if isinstance(s, (Session, AsyncSession)):
        return s.get_bind(clause=getattr(q, "statement", q))
    elif isinstance(s, (Engine, Connection)):
        return s
    elif isinstance(s, (AsyncEngine, AsyncConnection)):
        return s.sync_engine
    else:
        raise ValueError(f"{s} is not a (sync/async) Engine, Connection or Session.")


def get_session(s: Union[Engine, Connection, Session, ScopedSession]) -> Session:
    if isinstance(s, Session):
        return s
    elif isinstance(s, ScopedSession):
        return s()
    elif isinstance(s, (Engine, Connection)):
        return Session(bind=s)
    else:
        raise ValueError(f"{s} is not an Engine, Connection or Session.")


if SQLA_VERSION < version.parse("1.4.0b1"):
    from .sqla13 import (
        Row,
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )
elif SQLA_VERSION < version.parse("2.0.0b1"):
    from .sqla14 import (
        Row,
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )
else:
    from .sqla20 import (
        Row,
        core_coerce_row,
        core_result_type,
        group_by_clauses,
        order_by_clauses,
        orm_coerce_row,
        orm_query_keys,
        orm_result_type,
        orm_to_selectable,
    )

__all__ = [
    "core_coerce_row",
    "core_result_type",
    "get_bind",
    "group_by_clauses",
    "order_by_clauses",
    "orm_coerce_row",
    "orm_query_keys",
    "orm_result_type",
    "orm_to_selectable",
    "Row",
]
