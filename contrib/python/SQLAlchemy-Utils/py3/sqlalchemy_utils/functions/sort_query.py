import sqlalchemy as sa

from .database import has_unique_index
from .orm import _get_query_compile_state, get_tables


def make_order_by_deterministic(query):
    """
    Make query order by deterministic (if it isn't already). Order by is
    considered deterministic if it contains column that is unique index (
    either it is a primary key or has a unique index). Many times it is design
    flaw to order by queries in nondeterministic manner.

    Consider a User model with three fields: id (primary key), favorite color
    and email (unique).::


        from sqlalchemy_utils import make_order_by_deterministic


        query = session.query(User).order_by(User.favorite_color)

        query = make_order_by_deterministic(query)
        print query  # 'SELECT ... ORDER BY "user".favorite_color, "user".id'


        query = session.query(User).order_by(User.email)

        query = make_order_by_deterministic(query)
        print query  # 'SELECT ... ORDER BY "user".email'


        query = session.query(User).order_by(User.id)

        query = make_order_by_deterministic(query)
        print query  # 'SELECT ... ORDER BY "user".id'


    .. versionadded: 0.27.1
    """
    order_by_func = sa.asc

    try:
        order_by_clauses = query._order_by_clauses
    except AttributeError:  # SQLAlchemy <1.4
        order_by_clauses = query._order_by
    if not order_by_clauses:
        column = None
    else:
        order_by = order_by_clauses[0]
        if isinstance(order_by, sa.sql.elements._label_reference):
            order_by = order_by.element
        if isinstance(order_by, sa.sql.expression.UnaryExpression):
            if order_by.modifier == sa.sql.operators.desc_op:
                order_by_func = sa.desc
            else:
                order_by_func = sa.asc
            column = list(order_by.get_children())[0]
        else:
            column = order_by

    # Skip queries that are ordered by an already deterministic column
    if isinstance(column, sa.Column):
        try:
            if has_unique_index(column):
                return query
        except TypeError:
            pass

    base_table = get_tables(_get_query_compile_state(query)._entities[0])[0]
    query = query.order_by(
        *(order_by_func(c) for c in base_table.c if c.primary_key)
    )
    return query
