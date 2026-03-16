from typing import Any, NamedTuple

from sqllineage.core.models import Column, SubQuery, Table


class SubQueryTuple(NamedTuple):
    parenthesis: Any
    alias: str | None


class ColumnQualifierTuple(NamedTuple):
    column: str
    qualifier: str | None


class AnalyzerContext(NamedTuple):
    # CTE queries that can be select from in current query context
    cte: set[SubQuery] | None = None
    # table that current top-level query is writing to, subquery in case of subquery context
    write: set[SubQuery | Table] | None = None
    # columns that write table specifies, used for `INSERT INTO x (col1, col2) SELECT` syntax
    write_columns: list[Column] | None = None


class EdgeTuple(NamedTuple):
    source: Any
    target: Any
    label: str
    attributes: dict[str, Any] = {}
