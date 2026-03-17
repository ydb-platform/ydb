import datetime
from decimal import Decimal
from hashlib import sha1
from time import time
from typing import TYPE_CHECKING
from uuid import UUID

from django.contrib.postgres.functions import TransactionNow
from django.db.models import Exists, QuerySet, Subquery
from django.db.models.enums import Choices
from django.db.models.expressions import RawSQL
from django.db.models.functions import Now
from django.db.models.sql import Query, AggregateQuery
from django.db.models.sql.where import ExtraWhere, WhereNode, NothingNode

from .settings import ITERABLES, cachalot_settings
from .transaction import AtomicCache


if TYPE_CHECKING:
    from django.db.models.expressions import BaseExpression 


class UncachableQuery(Exception):
    pass


class IsRawQuery(Exception):
    pass


CACHABLE_PARAM_TYPES = {
    bool, int, float, Decimal, bytearray, bytes, str, type(None),
    datetime.date, datetime.time, datetime.datetime, datetime.timedelta, UUID,
}
UNCACHABLE_FUNCS = {Now, TransactionNow}


try:
    from psycopg.dbapi20 import Binary

    from django.db.backends.postgresql.psycopg_any import (
        NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet,
    )
    
    from psycopg.types.numeric import (
        Int2, Int4, Int8, Float4, Float8,
    )

    from ipaddress import (
        IPv4Address,
        IPv6Address,
    )

    from psycopg.types.json import (
        Json, Jsonb,
    )

    CACHABLE_PARAM_TYPES.update((
        NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet, Json, Jsonb,
        Int2, Int4, Int8, Float4, Float8, IPv4Address, IPv6Address,
        Binary,
    ))
except ImportError:
    try:
        from psycopg2 import Binary
        from psycopg2.extras import (
            NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet, Json)
        CACHABLE_PARAM_TYPES.update((
            Binary, NumericRange, DateRange, DateTimeRange, DateTimeTZRange, Inet,
            Json,))
    except ImportError:
        pass


def check_parameter_types(params):
    for p in params:
        cl = p.__class__
        if cl not in CACHABLE_PARAM_TYPES:
            if cl in ITERABLES:
                check_parameter_types(p)
            elif cl is dict:
                check_parameter_types(p.items())
            elif issubclass(cl, Choices):
                # Handle the case where a parameter from a Choices field is passed.
                # Django Choices use enum.unique() so the values are guaranteed to be unique.
                # Dereference the true "value" and verify that it is cachable.
                check_parameter_types([p.value])
            else:
                raise UncachableQuery


def get_query_cache_key(compiler):
    """
    Generates a cache key from a SQLCompiler.

    This cache key is specific to the SQL query and its context
    (which database is used).  The same query in the same context
    (= the same database) must generate the same cache key.

    :arg compiler: A SQLCompiler that will generate the SQL query
    :type compiler: django.db.models.sql.compiler.SQLCompiler
    :return: A cache key
    :rtype: int
    """
    sql, params = compiler.as_sql()
    check_parameter_types(params)
    cache_key = '%s:%s:%s' % (compiler.using, sql,
                              [str(p) for p in params])
    # Set attribute on compiler for later access
    # to the generated SQL. This prevents another as_sql() call!
    compiler.__cachalot_generated_sql = sql.lower()

    return sha1(cache_key.encode('utf-8')).hexdigest()


def get_table_cache_key(db_alias, table):
    """
    Generates a cache key from a SQL table.

    :arg db_alias: Alias of the used database
    :type db_alias: str or unicode
    :arg table: Name of the SQL table
    :type table: str or unicode
    :return: A cache key
    :rtype: int
    """
    cache_key = '%s:%s' % (db_alias, table)
    return sha1(cache_key.encode('utf-8')).hexdigest()


def _get_tables_from_sql(connection, lowercased_sql, enable_quote: bool = False):
    """Returns names of involved tables after analyzing the final SQL query."""
    return {table for table in (connection.introspection.django_table_names()
            + cachalot_settings.CACHALOT_ADDITIONAL_TABLES)
            if _quote_table_name(table, connection, enable_quote) in lowercased_sql}


def _quote_table_name(table_name, connection, enable_quote: bool):
    """
    Returns quoted table name.

    Put database-specific quotation marks around the table name
    to preven that tables with substrings of the table are considered.
    E.g. cachalot_testparent must not return cachalot_test.
    """
    return f'{connection.ops.quote_name(table_name)}' \
        if enable_quote else table_name


def _find_rhs_lhs_subquery(side):
    h_class = side.__class__
    if h_class is Query:
        return side
    elif h_class is QuerySet:
        return side.query
    elif h_class in (Subquery, Exists):  # Subquery allows QuerySet & Query
        return side.query.query if side.query.__class__ is QuerySet else side.query
    elif h_class in UNCACHABLE_FUNCS:
        raise UncachableQuery


def _find_subqueries_in_where(children):
    for child in children:
        child_class = child.__class__
        if child_class is WhereNode:
            for grand_child in _find_subqueries_in_where(child.children):
                yield grand_child
        elif child_class is ExtraWhere:
            raise IsRawQuery
        elif child_class is NothingNode:
            pass
        else:
            try:
                child_rhs = child.rhs
                child_lhs = child.lhs
            except AttributeError:
                raise UncachableQuery
            rhs = _find_rhs_lhs_subquery(child_rhs)
            if rhs is not None:
                yield rhs
            lhs = _find_rhs_lhs_subquery(child_lhs)
            if lhs is not None:
                yield lhs


def is_cachable(table):
    whitelist = cachalot_settings.CACHALOT_ONLY_CACHABLE_TABLES
    if whitelist and table not in whitelist:
        return False
    return table not in cachalot_settings.CACHALOT_UNCACHABLE_TABLES


def are_all_cachable(tables):
    whitelist = cachalot_settings.CACHALOT_ONLY_CACHABLE_TABLES
    if whitelist and not tables.issubset(whitelist):
        return False
    return tables.isdisjoint(cachalot_settings.CACHALOT_UNCACHABLE_TABLES)


def filter_cachable(tables):
    whitelist = cachalot_settings.CACHALOT_ONLY_CACHABLE_TABLES
    tables = tables.difference(cachalot_settings.CACHALOT_UNCACHABLE_TABLES)
    if whitelist:
        return tables.intersection(whitelist)
    return tables


def _flatten(expression: 'BaseExpression'):
    """
    Recursively yield this expression and all subexpressions, in
    depth-first order.

    Taken from Django 3.2 as the previous Django versions don’t check
    for existence of flatten.
    """
    yield expression
    for expr in expression.get_source_expressions():
        if expr:
            if hasattr(expr, 'flatten'):
                yield from _flatten(expr)
            else:
                yield expr


def _get_tables(db_alias, query, compiler=False):
    from django.db import connections

    if query.select_for_update or (
            not cachalot_settings.CACHALOT_CACHE_RANDOM
            and '?' in query.order_by):
        raise UncachableQuery

    try:
        if query.extra_select:
            raise IsRawQuery

        # Gets all tables already found by the ORM.
        tables = set(query.table_map)
        if query.get_meta():
            tables.add(query.get_meta().db_table)

        # Gets tables in subquery annotations.
        for annotation in query.annotations.values():
            if type(annotation) in UNCACHABLE_FUNCS:
                raise UncachableQuery
            for expression in _flatten(annotation):
                if isinstance(expression, Subquery):
                    # Django 2.2 only: no query, only queryset
                    if not hasattr(expression, 'query'):
                        tables.update(_get_tables(db_alias, expression.queryset.query))
                    # Django 3+
                    else:
                        tables.update(_get_tables(db_alias, expression.query))
                # Django 6.0+: Subquery.resolve_expression() returns Query directly
                # with subquery=True attribute instead of Subquery wrapper
                elif isinstance(expression, Query) and getattr(expression, 'subquery', False):
                    tables.update(_get_tables(db_alias, expression))
                elif isinstance(expression, RawSQL):
                    sql = expression.as_sql(None, None)[0].lower()
                    tables.update(_get_tables_from_sql(connections[db_alias], sql))
        # Gets tables in WHERE subqueries.
        for subquery in _find_subqueries_in_where(query.where.children):
            tables.update(_get_tables(db_alias, subquery))
        # Gets tables in HAVING subqueries.
        if isinstance(query, AggregateQuery):
            try:
                tables.update(_get_tables_from_sql(connections[db_alias], query.subquery))
            except TypeError:  # For Django 3.2+
                tables.update(_get_tables(db_alias, query.inner_query))
        # Gets tables in combined queries
        # using `.union`, `.intersection`, or `difference`.
        if query.combined_queries:
            for combined_query in query.combined_queries:
                tables.update(_get_tables(db_alias, combined_query))
    except IsRawQuery:
        sql = query.get_compiler(db_alias).as_sql()[0].lower()
        tables = _get_tables_from_sql(connections[db_alias], sql)
    else:
        # Additional check of the final SQL.
        # Potentially overlooked tables are added here. Tables may be overlooked by the regular checks
        # as not all expressions are handled yet. This final check acts as safety net.
        if cachalot_settings.CACHALOT_FINAL_SQL_CHECK:
            if compiler:
                # Access generated SQL stored when caching the query!
                sql = compiler.__cachalot_generated_sql
            else:
                sql = query.get_compiler(db_alias).as_sql()[0].lower()
            final_check_tables = _get_tables_from_sql(connections[db_alias], sql, enable_quote=True)
            tables.update(final_check_tables)

    if not are_all_cachable(tables):
        raise UncachableQuery
    return tables


def _get_table_cache_keys(compiler):
    db_alias = compiler.using
    get_table_cache_key = cachalot_settings.CACHALOT_TABLE_KEYGEN
    return [get_table_cache_key(db_alias, t)
            for t in _get_tables(db_alias, compiler.query, compiler)]


def _invalidate_tables(cache, db_alias, tables):
    tables = filter_cachable(set(tables))
    if not tables:
        return
    now = time()
    get_table_cache_key = cachalot_settings.CACHALOT_TABLE_KEYGEN
    cache.set_many(
        {get_table_cache_key(db_alias, t): now for t in tables},
        cachalot_settings.CACHALOT_TIMEOUT)

    if isinstance(cache, AtomicCache):
        cache.to_be_invalidated.update(tables)
