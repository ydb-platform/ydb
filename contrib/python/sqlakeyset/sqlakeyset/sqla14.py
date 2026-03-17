"""Methods for messing with the internals of SQLAlchemy >1.3 results."""

from __future__ import annotations

from sqlalchemy.engine.result import SimpleResultMetaData
from sqlalchemy.engine.row import LegacyRow

from .constants import ORDER_COL_PREFIX
from .sqla20 import (
    Row,
    TruncatedRow,
    core_result_type,
    group_by_clauses,
    order_by_clauses,
    orm_coerce_row,
    orm_query_keys,
    orm_result_type,
    orm_to_selectable,
    result_keys,
)


class TruncatedLegacyRow(LegacyRow):
    def keys(self):
        return result_keys(self._parent)


def core_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row.

    In SQLAlchemy 1.4 there is the possibility of the row being a transitional
    LegacyRow, so we handle this case explicitly."""
    if not extra_columns:
        return row
    N = len(row) - len(extra_columns)

    if isinstance(row, LegacyRow):
        cls = TruncatedLegacyRow
    else:
        cls = TruncatedRow

    parent = row._parent
    if isinstance(parent, SimpleResultMetaData):
        parent = parent._reduce(list(parent.keys)[:N])

    return cls(
        parent,
        None,  # Processors are applied immediately in sqla1.4+
        {  # Strip out added OCs from the keymap:
            k: v
            for k, v in row._keymap.items()
            if not (isinstance(v[1], str) and v[1].startswith(ORDER_COL_PREFIX))
        },
        row._key_style,
        row._data[:N],
    )


__all__ = [
    "Row",
    "TruncatedRow",
    "core_coerce_row",
    "core_result_type",
    "group_by_clauses",
    "order_by_clauses",
    "orm_coerce_row",
    "orm_query_keys",
    "orm_result_type",
    "orm_to_selectable",
]
