from typing import Optional

from sqlalchemy import and_, true
from sqlalchemy.sql.base import Immutable
from sqlalchemy.sql.selectable import FromClause, Join
from sqlalchemy.sql.visitors import InternalTraversal


def _normalize_array_columns(array_column, alias):
    """Normalize single/multi column input into a list of (column, alias_or_none) tuples."""
    if isinstance(array_column, (list, tuple)):
        columns = list(array_column)
        if not columns:
            raise ValueError("At least one array column is required")
        if alias is None:
            aliases = [None] * len(columns)
        elif isinstance(alias, (list, tuple)):
            aliases = list(alias)
            if len(aliases) != len(columns):
                raise ValueError(f"Length of alias list ({len(aliases)}) must match " f"length of array_column list ({len(columns)})")
        else:
            raise ValueError("alias must be a list when array_column is a list")
    else:
        columns = [array_column]
        if isinstance(alias, (list, tuple)):
            raise ValueError("alias must be a string or None when array_column is a single column")
        aliases = [alias]

    return list(zip(columns, aliases))


# pylint: disable=protected-access,too-many-ancestors,abstract-method,unused-argument
class ArrayJoin(Immutable, FromClause):
    """Represents ClickHouse ARRAY JOIN clause.

    Supports single or multiple array columns with optional per-column aliases.
    Multiple columns are expanded in parallel (zipped by position), not as a
    cartesian product. All arrays in a single ARRAY JOIN must have the same
    length per row unless enable_unaligned_array_join is set on the server.

    See: https://clickhouse.com/docs/sql-reference/statements/select/array-join
    """

    __visit_name__ = "array_join"
    _is_from_container = True
    named_with_column = False
    _is_join = True

    def __init__(self, left, array_column, alias=None, is_left=False):
        """Initialize ARRAY JOIN clause.

        Args:
            left: The left side (table or subquery).
            array_column: A single array column, or a list/tuple of array columns.
            alias: Optional alias. A single string when array_column is a single
                column, or a list/tuple of strings (same length as array_column)
                when array_column is a list. None means no aliases.
            is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN.
        """
        super().__init__()
        self.left = left
        self.array_columns = _normalize_array_columns(array_column, alias)
        self.is_left = is_left
        self._is_clone_of = None

    @property
    def selectable(self):
        """Return the selectable for this clause"""
        return self.left

    @property
    def _hide_froms(self):
        """Hide the left table from the FROM clause since it's part of the ARRAY JOIN"""
        return [self.left]

    @property
    def _from_objects(self):
        """Return all FROM objects referenced by this construct"""
        return self.left._from_objects

    def _clone(self, **kw):
        """Return a copy of this ArrayJoin"""
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        c._is_clone_of = self
        return c

    def _copy_internals(self, clone=None, **kw):
        """Copy internal state for cloning.

        This ensures that when queries are cloned (e.g., for subqueries, unions, or CTEs),
        the left FromClause and array column references are properly deep-cloned.
        """
        def _default_clone(elem, **kwargs):
            return elem

        if clone is None:
            clone = _default_clone

        self.left = clone(self.left, **kw)
        self.array_columns = [
            (clone(col, **kw), alias)
            for col, alias in self.array_columns
        ]


def array_join(left, array_column, alias=None, is_left=False):
    """Create an ARRAY JOIN clause.

    Supports single or multiple array columns. When multiple columns are
    provided, they are expanded in parallel (zipped by index position).

    Args:
        left: The left side (table or subquery).
        array_column: A single array column, or a list/tuple of array columns.
        alias: Optional alias. A single string when array_column is a single
            column, or a list/tuple of strings (same length as array_column)
            when array_column is a list. None means no aliases.
        is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN.

    Returns:
        ArrayJoin: An ArrayJoin clause element.

    Examples:
        from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join

        # Single column ARRAY JOIN
        query = select(table).select_from(array_join(table, table.c.tags))

        # Single column LEFT ARRAY JOIN with alias
        query = select(table).select_from(
            array_join(table, table.c.tags, alias="tag", is_left=True)
        )

        # Multiple columns with aliases
        query = select(table).select_from(
            array_join(
                table,
                [table.c.names, table.c.prices, table.c.quantities],
                alias=["name", "price", "quantity"],
            )
        )
    """
    return ArrayJoin(left, array_column, alias, is_left)


_VALID_STRICTNESS = frozenset({None, "ALL", "ANY", "SEMI", "ANTI", "ASOF"})
_VALID_DISTRIBUTION = frozenset({None, "GLOBAL"})


def _validate_ch_join(strictness, distribution, onclause, isouter, full, is_cross, using):
    """Validate ClickHouse join parameter combinations."""
    if strictness not in _VALID_STRICTNESS:
        raise ValueError(f"Invalid strictness {strictness!r}. Must be one of: ALL, ANY, SEMI, ANTI, ASOF")
    if distribution not in _VALID_DISTRIBUTION:
        raise ValueError(f"Invalid distribution {distribution!r}. Must be: GLOBAL")
    if is_cross and strictness is not None:
        raise ValueError("Strictness modifiers cannot be used with CROSS JOIN")
    if is_cross and (isouter or full):
        raise ValueError("CROSS JOIN cannot be combined with isouter or full")
    if strictness in ("SEMI", "ANTI") and not isouter:
        raise ValueError(f"{strictness} JOIN requires isouter=True (LEFT) or swapped table order (RIGHT)")
    if strictness == "ASOF" and full:
        raise ValueError("ASOF is not supported with FULL joins")
    if using is not None:
        if is_cross:
            raise ValueError("USING cannot be combined with CROSS JOIN")
        if onclause is not None:
            raise ValueError("Cannot specify both onclause and using")
        if not isinstance(using, (list, tuple)) or not using:
            raise ValueError("using must be a non-empty list of column name strings")
        if not all(isinstance(col, str) for col in using):
            raise ValueError("using must contain only column name strings")


def _build_using_onclause(left, right, using):
    """Build an equality onclause from USING column names.

    This gives SQLAlchemy's from-linter proper column references so it
    knows the tables are connected. The compiler renders USING instead of ON.
    """
    conditions = []
    for col in using:
        try:
            conditions.append(left.c[col] == right.c[col])
        except KeyError:
            left_cols = {c.name for c in left.c}
            right_cols = {c.name for c in right.c}
            missing_from = []
            if col not in left_cols:
                missing_from.append(str(left))
            if col not in right_cols:
                missing_from.append(str(right))
            raise ValueError(f"USING column {col!r} not found in: {', '.join(missing_from)}") from None
    return and_(*conditions) if len(conditions) > 1 else conditions[0]


# pylint: disable=too-many-ancestors,abstract-method
class ClickHouseJoin(Join):
    """A SQLAlchemy Join subclass that supports ClickHouse-specific join features.

    ClickHouse JOIN syntax: [GLOBAL] [ALL|ANY|SEMI|ANTI|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] JOIN

    Strictness modifiers control how multiple matches are handled:
        - ALL: return all matching rows (default, standard SQL behavior)
        - ANY: return only the first match per left row
        - SEMI: acts as an allowlist on join keys, no Cartesian product
        - ANTI: acts as a denylist on join keys, no Cartesian product
        - ASOF: time-series join, finds the closest match

    Distribution modifier:
        - GLOBAL: broadcasts the right table to all nodes in distributed queries

    USING clause:
        - Joins on same-named columns from both tables. Unlike ON, USING merges
          matched columns into one, which is important for FULL OUTER JOIN where
          ON produces default values (0, '') for unmatched sides.

    Note: RIGHT JOIN is achieved by swapping table order, which is standard SQLAlchemy behavior.
    ASOF JOIN requires the last ON condition to be an inequality which is validated by
    the ClickHouse server, not here. Not all strictness/join type combinations are supported
    by every join algorithm and the server will report unsupported combinations.
    """

    __visit_name__ = "join"

    _traverse_internals = Join._traverse_internals + [
        ("strictness", InternalTraversal.dp_string),
        ("distribution", InternalTraversal.dp_string),
        ("_is_cross", InternalTraversal.dp_boolean),
        ("using_columns", InternalTraversal.dp_string_list),
    ]

    def __init__(self, left, right, onclause=None, isouter=False, full=False,
                 strictness=None, distribution=None, _is_cross=False, using=None):
        if strictness is not None:
            strictness = strictness.upper()
        if distribution is not None:
            distribution = distribution.upper()

        _validate_ch_join(strictness, distribution, onclause, isouter, full, _is_cross, using)

        effective_onclause = _build_using_onclause(left, right, using) if using else onclause
        super().__init__(left, right, effective_onclause, isouter, full)
        self.strictness = strictness
        self.distribution = distribution
        self._is_cross = _is_cross
        self.using_columns = list(using) if using is not None else None


def ch_join(
    left,
    right,
    onclause=None,
    *,
    isouter=False,
    full=False,
    cross=False,
    using=None,
    strictness: Optional[str] = None,
    distribution: Optional[str] = None,
):
    """Create a ClickHouse JOIN with optional strictness, distribution, and USING support.

    Args:
        left: The left side table or selectable.
        right: The right side table or selectable.
        onclause: The ON clause expression. Mutually exclusive with ``using``.
        isouter: If True, render a LEFT OUTER JOIN.
        full: If True, render a FULL OUTER JOIN.
        cross: If True, render a CROSS JOIN. Cannot be combined with
            onclause, using, or strictness modifiers.
        using: A list of column name strings for USING syntax. The columns
            must have the same name in both tables. Mutually exclusive with
            ``onclause``. Produces ``USING (col1, col2)`` instead of ``ON``.
        strictness: ClickHouse strictness modifier, one of
            "ALL", "ANY", "SEMI", "ANTI", or "ASOF".
        distribution: ClickHouse distribution modifier "GLOBAL".

    Returns:
        ClickHouseJoin: A join element with ClickHouse modifiers.
    """
    if cross:
        if onclause is not None:
            raise ValueError("cross=True conflicts with an explicit onclause")
        if using is not None:
            raise ValueError("cross=True conflicts with using")
        onclause = true()
    return ClickHouseJoin(left, right, onclause, isouter, full,
                          strictness, distribution, _is_cross=cross, using=using)
