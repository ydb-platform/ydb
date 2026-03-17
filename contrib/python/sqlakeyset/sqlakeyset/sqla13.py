"""Methods for messing with the internals of SQLAlchemy <=1.3 results."""

from __future__ import annotations

from typing import TypeVar

from sqlalchemy.util import lightweight_named_tuple
from typing_extensions import Protocol


def orm_result_type(query):
    """Return the type constructor for rows that would be returned by a given
    query; or the identity function for queries that return a single entity.

    :param query: The query to inspect.
    :type query: :class:`sqlalchemy.orm.query.Query`.
    :returns: either a named tuple type or the identity."""

    if query.is_single_entity:
        return lambda x: x[0]
    labels = [e._label_name for e in query._entities]
    return lightweight_named_tuple("result", labels)


_T = TypeVar("_T", covariant=True)


class Row(Protocol[_T]):
    """This is a workaround for typechecking errors in sqla13."""


def orm_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns."""
    # orm_get_page might have added some extra columns to the query in order
    # to get the keys for the bookmark. Here, we split the rows back to the
    # requested data and the ordering keys.
    N = len(row) - len(extra_columns)
    return result_type(row[:N])


def core_result_type(selectable, s):
    result_proxy = s.execute(selectable.limit(0))

    def clean_row(row):
        # This is copied from the internals of sqlalchemy's ResultProxy.
        process_row = result_proxy._process_row
        metadata = result_proxy._metadata
        keymap = metadata._keymap
        processors = metadata._processors
        return process_row(metadata, row, processors, keymap)

    return clean_row


def core_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row."""
    if not extra_columns:
        return row
    N = len(row._row) - len(extra_columns)
    return result_type(row._row[:N])


def orm_query_keys(query):
    return [e._label_name for e in query._entities]


def orm_to_selectable(q):
    """Normalize an ORM query into a selectable."""
    return q.selectable


def order_by_clauses(selectable):
    """Extract the ORDER BY clause list from a select/query"""
    return selectable._order_by_clause


def group_by_clauses(selectable):
    """Extract the GROUP BY clause list from a select/query"""
    return selectable._group_by_clause


__all__ = [
    "Row",
    "core_coerce_row",
    "core_result_type",
    "group_by_clauses",
    "order_by_clauses",
    "orm_coerce_row",
    "orm_query_keys",
    "orm_result_type",
    "orm_to_selectable",
]
