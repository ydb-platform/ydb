from typing import Any, NamedTuple, Optional


class SubQueryTuple(NamedTuple):
    parenthesis: Any
    alias: Optional[str]


class ColumnQualifierTuple(NamedTuple):
    column: str
    qualifier: Optional[str]
