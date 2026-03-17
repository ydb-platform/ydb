"""Methods for messing with the internals of SQLAlchemy 1.4/2.0 results."""

from __future__ import annotations

from sqlalchemy.engine.result import SimpleResultMetaData, result_tuple
from sqlalchemy.engine.row import Row

from .constants import ORDER_COL_PREFIX


def orm_query_keys(query):
    """Given a SQLAlchemy ORM query, extract the list of column keys expected
    in the result."""
    return [c["name"] for c in query.column_descriptions]


def _create_result_tuple(compile_state):
    """Create a `_row_getter` function to use as `result_type`"""
    # This is adapted from the internals of sqlalchemy.orm.loading
    keys = [ent._label_name for ent in compile_state._entities]

    keyed_tuple = result_tuple(
        keys, [ent._extra_entities for ent in compile_state._entities]
    )
    return keyed_tuple


def _is_single_entity(compile_state, query_context):
    # This is copied from the internals of sqlalchemy.orm.loading
    return (
        not query_context.load_options._only_return_tuples
        and len(compile_state._entities) == 1
        and compile_state._entities[0].supports_single_entity
    )


def orm_result_type(query):
    """Return the type constructor for rows that would be returned by a given
    query; or the identity function for queries that return a single entity
    rather than rows.

    :param query: The query to inspect.
    :type query: :class:`sqlalchemy.orm.query.Query`.
    :returns: either a named tuple type or the identity."""

    state = query._compile_state()
    if _is_single_entity(state, query._compile_context()):
        return lambda x: x[0]
    return _create_result_tuple(state)


def orm_coerce_row(row, extra_columns, result_type):
    """Trim off the extra columns."""
    # orm_get_page might have added some extra columns to the query in order
    # to get the keys for the bookmark. Here, we split the rows back to the
    # requested data and the ordering keys.
    N = len(row) - len(extra_columns)
    return result_type(row[:N])


def core_result_type(selectable, s):
    """Given a SQLAlchemy Core selectable and a connection/session, get the
    type constructor for the result row type."""
    # Unused in sqlalchemy 1.4+: see core_coerce_row implementation
    return None


def result_keys(result):
    return [k for k in result.keys if not k.startswith(ORDER_COL_PREFIX)]


class TruncatedRow(Row):
    def keys(self):
        return result_keys(self._parent)


def orm_to_selectable(q):
    """Normalize an ORM query into a selectable.

    In SQLAlchemy 1.4, there is no distinction."""
    return q


def order_by_clauses(selectable):
    """Extract the ORDER BY clause list from a select/query"""
    return selectable._order_by_clauses


def group_by_clauses(selectable):
    """Extract the GROUP BY clause list from a select/query"""
    return selectable._group_by_clauses


def core_coerce_row(row: Row, extra_columns, result_type) -> Row:
    """Trim off the extra columns and return as a correct-as-possible
    sqlalchemy Row."""
    if not extra_columns:
        return row
    N = len(row) - len(extra_columns)

    parent = row._parent
    if isinstance(parent, SimpleResultMetaData):
        parent = parent._reduce(list(parent.keys)[:N])
    processors = None  # Processors are applied immediately in sqla1.4+
    data = row._data[:N]

    if hasattr(row, "_key_to_index"):
        # 2.0.11+
        structure = (
            {  # Strip out added OCs from the keymap:
                k: v
                for k, v in row._key_to_index.items()
                if not (isinstance(k, str) and k.startswith(ORDER_COL_PREFIX))
            },
        )
    else:
        # <2.0.11
        structure = (
            {  # Strip out added OCs from the keymap:
                k: v
                for k, v in row._keymap.items()
                if not (isinstance(v[1], str) and v[1].startswith(ORDER_COL_PREFIX))
            },
            row._key_style,
        )

    return TruncatedRow(
        parent,
        processors,
        *structure,
        data,
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
    "result_keys",
]
