from collections.abc import Callable
from typing import Any

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql.compiler import IdentifierPreparer

from clickhouse_connect.driver.binding import escape_str
from clickhouse_connect.driver.binding import quote_identifier as ch_quote_identifier


class ChIdentifierPreparer(IdentifierPreparer):
    """ClickHouse identifier quoting for SQLAlchemy compiler output."""

    # Keep the existing public helper contract. Direct callers get one literal
    # percent sign, while quote() handles pyformat escaping for compiled SQL.
    quote_identifier: Callable[[str], str] = staticmethod(ch_quote_identifier)  # type: ignore[assignment]

    def __init__(self, dialect: Dialect, **kwargs: Any) -> None:
        super().__init__(dialect, **kwargs)
        if getattr(dialect, "server_side_params", False):
            self._double_percents = False

    def _escape_percents(self, identifier: str) -> str:
        if self._double_percents:
            return identifier.replace("%", "%%")
        return identifier

    def quote(self, ident: str, force: Any = None) -> str:
        return self._escape_percents(super().quote(ident))

    def _quote_raw_identifier(self, value: str) -> str:
        """Quote raw identifier content even when it already looks quoted."""
        return self._escape_percents(f"`{escape_str(value)}`")

    def _requires_quotes(self, _value: str) -> bool:
        return True
