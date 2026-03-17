from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SqlContext:
    """Represents the context for get_sql() methods to determine how to render SQL."""

    quote_char: str
    secondary_quote_char: str
    alias_quote_char: str
    dialect: Dialects
    as_keyword: bool = False
    subquery: bool = False
    with_alias: bool = False
    with_namespace: bool = False
    subcriterion: bool = False
    parameterizer: Parameterizer | None = None
    groupby_alias: bool = True
    orderby_alias: bool = True

    def copy(self, **kwargs) -> SqlContext:
        return SqlContext(
            quote_char=kwargs.get("quote_char", self.quote_char),
            secondary_quote_char=kwargs.get("secondary_quote_char", self.secondary_quote_char),
            alias_quote_char=kwargs.get("alias_quote_char", self.alias_quote_char),
            dialect=kwargs.get("dialect", self.dialect),
            as_keyword=kwargs.get("as_keyword", self.as_keyword),
            subquery=kwargs.get("subquery", self.subquery),
            with_alias=kwargs.get("with_alias", self.with_alias),
            with_namespace=kwargs.get("with_namespace", self.with_namespace),
            subcriterion=kwargs.get("subcriterion", self.subcriterion),
            parameterizer=kwargs.get("parameterizer", self.parameterizer),
            groupby_alias=kwargs.get("groupby_alias", self.groupby_alias),
            orderby_alias=kwargs.get("orderby_alias", self.orderby_alias),
        )


from .enums import Dialects  # noqa: E402

DEFAULT_SQL_CONTEXT = SqlContext(
    quote_char='"',
    secondary_quote_char="'",
    alias_quote_char="",
    as_keyword=False,
    dialect=Dialects.SQLITE,
)

from .terms import Parameterizer  # noqa: E402
