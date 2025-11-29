from sqlalchemy.sql.base import Immutable
from sqlalchemy.sql.selectable import FromClause


# pylint: disable=protected-access,too-many-ancestors,abstract-method,unused-argument
class ArrayJoin(Immutable, FromClause):
    """Represents ClickHouse ARRAY JOIN clause"""

    __visit_name__ = "array_join"
    _is_from_container = True
    named_with_column = False
    _is_join = True

    def __init__(self, left, array_column, alias=None, is_left=False):
        """Initialize ARRAY JOIN clause

        Args:
            left: The left side (table or subquery)
            array_column: The array column to join
            alias: Optional alias for the joined array elements
            is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN
        """
        super().__init__()
        self.left = left
        self.array_column = array_column
        self.alias = alias
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
        """Copy internal state for cloning

        This ensures that when queries are cloned (e.g., for subqueries, unions, or CTEs),
        the left FromClause and array_column references are properly deep-cloned.
        """
        def _default_clone(elem, **kwargs):
            return elem

        if clone is None:
            clone = _default_clone

        # Clone the left FromClause and array column to ensure proper
        #  reference handling in complex query scenarios
        self.left = clone(self.left, **kw)
        self.array_column = clone(self.array_column, **kw)


def array_join(left, array_column, alias=None, is_left=False):
    """Create an ARRAY JOIN clause

    Args:
        left: The left side (table or subquery)
        array_column: The array column to join
        alias: Optional alias for the joined array elements
        is_left: If True, use LEFT ARRAY JOIN instead of ARRAY JOIN

    Returns:
        ArrayJoin: An ArrayJoin clause element

    Example:
        from clickhouse_connect.cc_sqlalchemy.sql.clauses import array_join

        # Basic ARRAY JOIN
        query = select(table).select_from(array_join(table, table.c.tags))

        # LEFT ARRAY JOIN with alias
        query = select(table).select_from(
            array_join(table, table.c.tags, alias='tag', is_left=True)
        )
    """
    return ArrayJoin(left, array_column, alias, is_left)
