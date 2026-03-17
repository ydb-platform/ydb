from __future__ import annotations

from typing import Any

from ..context import DEFAULT_SQL_CONTEXT, SqlContext
from ..enums import Dialects
from ..queries import Query, QueryBuilder
from ..terms import ValueWrapper


class SQLLiteValueWrapper(ValueWrapper):
    def get_value_sql(self, ctx: SqlContext) -> str:
        if isinstance(self.value, bool):
            return "1" if self.value else "0"
        return super().get_value_sql(ctx)


class SQLLiteQuery(Query):
    """
    Defines a query class for use with Microsoft SQL Server.
    """

    SQL_CONTEXT = DEFAULT_SQL_CONTEXT.copy(dialect=Dialects.SQLITE)

    @classmethod
    def _builder(cls, **kwargs: Any) -> SQLLiteQueryBuilder:
        return SQLLiteQueryBuilder(**kwargs)


class SQLLiteQueryBuilder(QueryBuilder):
    QUERY_CLS = SQLLiteQuery

    def __init__(self, **kwargs) -> None:
        super().__init__(wrapper_cls=SQLLiteValueWrapper, **kwargs)

    def get_sql(self, ctx: SqlContext | None = None) -> str:
        ctx = ctx or SQLLiteQuery.SQL_CONTEXT
        if not (self._selects or self._insert_table or self._delete_from or self._update_table):
            return ""
        if self._insert_table and not (self._selects or self._values):
            return ""
        if self._update_table and not self._updates:
            return ""

        has_joins = bool(self._joins)
        has_multiple_from_clauses = len(self._from) > 1
        has_subquery_from_clause = len(self._from) > 0 and isinstance(self._from[0], QueryBuilder)
        has_reference_to_foreign_table = self._foreign_table
        has_update_from = self._update_table and self._from

        ctx = ctx.copy(
            with_namespace=any(
                [
                    has_joins,
                    has_multiple_from_clauses,
                    has_subquery_from_clause,
                    has_reference_to_foreign_table,
                    has_update_from,
                ]
            ),
        )
        if self._update_table:
            querystring = self._with_sql(ctx) if self._with else ""

            querystring += self._update_sql(ctx)
            querystring += self._set_sql(ctx)

            if self._joins:
                self._from.append(self._update_table.as_(self._update_table.get_table_name() + "_"))

            if self._from:
                querystring += self._from_sql(ctx)
            if self._joins:
                querystring += " " + " ".join(join.get_sql(ctx) for join in self._joins)

            if self._wheres:
                querystring += self._where_sql(ctx)

            if self._orderbys:
                querystring += self._orderby_sql(ctx)
            if self._limit:
                querystring += self._limit_sql(ctx)
        else:
            querystring = super().get_sql(ctx=ctx)
        return querystring
