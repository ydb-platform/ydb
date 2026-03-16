from __future__ import annotations

from typing import Any

from ..context import DEFAULT_SQL_CONTEXT, SqlContext
from ..enums import Dialects
from ..queries import Query, QueryBuilder


class OracleQuery(Query):
    """
    Defines a query class for use with Oracle.
    """

    SQL_CONTEXT = DEFAULT_SQL_CONTEXT.copy(dialect=Dialects.ORACLE, alias_quote_char='"')

    @classmethod
    def _builder(cls, **kwargs: Any) -> OracleQueryBuilder:
        return OracleQueryBuilder(**kwargs)


class OracleQueryBuilder(QueryBuilder):
    QUERY_CLS = OracleQuery

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def get_sql(self, ctx: SqlContext | None = None) -> str:
        if not ctx:
            ctx = OracleQuery.SQL_CONTEXT
        # Oracle does not support group by a field alias.
        ctx = ctx.copy(groupby_alias=False)
        return super().get_sql(ctx)

    def _offset_sql(self, ctx: SqlContext) -> str:
        if self._offset is None:
            return ""
        return " OFFSET {offset} ROWS".format(offset=self._offset.get_sql(ctx))

    def _limit_sql(self, ctx: SqlContext) -> str:
        if self._limit is None:
            return ""
        return " FETCH NEXT {limit} ROWS ONLY".format(limit=self._limit.get_sql(ctx))
