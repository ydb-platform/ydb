from _typeshed import Incomplete
from collections.abc import Mapping
from typing import Any

MAX_QUERY_MESSAGE_LENGTH: int

def message_from_db_statement(attributes: Mapping[str, Any], message: str | None, span_name: str) -> str | None:
    """Try to construct a useful span message from OTel db statement.

    Returns: A new string to use as span message or None to keep the original message.
    """

TABLE_RE: str
SELECT_RE: Incomplete
SELECT_CTE_RE: Incomplete
SELECT_SUBQUERY_RE: Incomplete
INSERT_RE: Incomplete

def summarize_query(db_statement: str) -> str | None:
    """Summarize a database statement, specifically SQL queries.

    Args:
        db_statement: The database statement to summarize.

    Returns: A new string to use as span message or None to keep the original message.

    """
def select(expr: str, table: str, *, match_end: int, db_statement: str, ctes: str | None = None, sub_query: str | None = None) -> str: ...
def truncate(s: str, length: int) -> str: ...

FALLBACK_HALF: Incomplete

def fallback(db_statement: str): ...
