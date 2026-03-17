import contextlib
import re

from sqlalchemy import __version__
from sqlalchemy import inspect
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import url
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.schema import Column
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql.expression import _BindParamClause
from sqlalchemy.sql.expression import _TextClause as TextClause
from sqlalchemy.sql.visitors import traverse

from . import compat


def _safe_int(value):
    try:
        return int(value)
    except:
        return value


_vers = tuple(
    [_safe_int(x) for x in re.findall(r"(\d+|[abc]\d)", __version__)]
)
sqla_12 = _vers >= (1, 2)
sqla_13 = _vers >= (1, 3)
sqla_14 = _vers >= (1, 4)

try:
    from sqlalchemy import Computed  # noqa
except ImportError:
    has_computed = False
    has_computed_reflection = False
else:
    has_computed = True
    has_computed_reflection = _vers >= (1, 3, 16)

try:
    from sqlalchemy import Identity  # noqa
except ImportError:
    has_identity = False
else:
    # attributes common to Indentity and Sequence
    _identity_options_attrs = (
        "start",
        "increment",
        "minvalue",
        "maxvalue",
        "nominvalue",
        "nomaxvalue",
        "cycle",
        "cache",
        "order",
    )
    # attributes of Indentity
    _identity_attrs = _identity_options_attrs + ("on_null",)
    has_identity = True

AUTOINCREMENT_DEFAULT = "auto"


@contextlib.contextmanager
def _ensure_scope_for_ddl(connection):
    try:
        in_transaction = connection.in_transaction
    except AttributeError:
        # catch for MockConnection
        yield
    else:
        if not in_transaction():
            with connection.begin():
                yield
        else:
            yield


def _safe_begin_connection_transaction(connection):
    transaction = _get_connection_transaction(connection)
    if transaction:
        return transaction
    else:
        return connection.begin()


def _get_connection_in_transaction(connection):
    try:
        in_transaction = connection.in_transaction
    except AttributeError:
        # catch for MockConnection
        return False
    else:
        return in_transaction()


def _copy(schema_item, **kw):
    if hasattr(schema_item, "_copy"):
        return schema_item._copy(**kw)
    else:
        return schema_item.copy(**kw)


def _get_connection_transaction(connection):
    if sqla_14:
        return connection.get_transaction()
    else:
        return connection._root._Connection__transaction


def _create_url(*arg, **kw):
    if hasattr(url.URL, "create"):
        return url.URL.create(*arg, **kw)
    else:
        return url.URL(*arg, **kw)


def _connectable_has_table(connectable, tablename, schemaname):
    if sqla_14:
        return inspect(connectable).has_table(tablename, schemaname)
    else:
        return connectable.dialect.has_table(
            connectable, tablename, schemaname
        )


def _exec_on_inspector(inspector, statement, **params):
    if sqla_14:
        with inspector._operation_context() as conn:
            return conn.execute(statement, params)
    else:
        return inspector.bind.execute(statement, params)


def _nullability_might_be_unset(metadata_column):
    if not sqla_14:
        return metadata_column.nullable
    else:
        from sqlalchemy.sql import schema

        return (
            metadata_column._user_defined_nullable is schema.NULL_UNSPECIFIED
        )


def _server_default_is_computed(*server_default):
    if not has_computed:
        return False
    else:
        return any(isinstance(sd, Computed) for sd in server_default)


def _server_default_is_identity(*server_default):
    if not sqla_14:
        return False
    else:
        return any(isinstance(sd, Identity) for sd in server_default)


def _table_for_constraint(constraint):
    if isinstance(constraint, ForeignKeyConstraint):
        return constraint.parent
    else:
        return constraint.table


def _columns_for_constraint(constraint):
    if isinstance(constraint, ForeignKeyConstraint):
        return [fk.parent for fk in constraint.elements]
    elif isinstance(constraint, CheckConstraint):
        return _find_columns(constraint.sqltext)
    else:
        return list(constraint.columns)


def _reflect_table(inspector, table, include_cols):
    if sqla_14:
        return inspector.reflect_table(table, None)
    else:
        return inspector.reflecttable(table, None)


def _fk_spec(constraint):
    source_columns = [
        constraint.columns[key].name for key in constraint.column_keys
    ]

    source_table = constraint.parent.name
    source_schema = constraint.parent.schema
    target_schema = constraint.elements[0].column.table.schema
    target_table = constraint.elements[0].column.table.name
    target_columns = [element.column.name for element in constraint.elements]
    ondelete = constraint.ondelete
    onupdate = constraint.onupdate
    deferrable = constraint.deferrable
    initially = constraint.initially
    return (
        source_schema,
        source_table,
        source_columns,
        target_schema,
        target_table,
        target_columns,
        onupdate,
        ondelete,
        deferrable,
        initially,
    )


def _fk_is_self_referential(constraint):
    spec = constraint.elements[0]._get_colspec()
    tokens = spec.split(".")
    tokens.pop(-1)  # colname
    tablekey = ".".join(tokens)
    return tablekey == constraint.parent.key


def _is_type_bound(constraint):
    # this deals with SQLAlchemy #3260, don't copy CHECK constraints
    # that will be generated by the type.
    # new feature added for #3260
    return constraint._type_bound


def _find_columns(clause):
    """locate Column objects within the given expression."""

    cols = set()
    traverse(clause, {}, {"column": cols.add})
    return cols


def _remove_column_from_collection(collection, column):
    """remove a column from a ColumnCollection."""

    # workaround for older SQLAlchemy, remove the
    # same object that's present
    to_remove = collection[column.key]
    collection.remove(to_remove)


def _textual_index_column(table, text_):
    """a workaround for the Index construct's severe lack of flexibility"""
    if isinstance(text_, compat.string_types):
        c = Column(text_, sqltypes.NULLTYPE)
        table.append_column(c)
        return c
    elif isinstance(text_, TextClause):
        return _textual_index_element(table, text_)
    elif isinstance(text_, sql.ColumnElement):
        return _copy_expression(text_, table)
    else:
        raise ValueError("String or text() construct expected")


def _copy_expression(expression, target_table):
    def replace(col):
        if (
            isinstance(col, Column)
            and col.table is not None
            and col.table is not target_table
        ):
            if col.name in target_table.c:
                return target_table.c[col.name]
            else:
                c = _copy(col)
                target_table.append_column(c)
                return c
        else:
            return None

    return visitors.replacement_traverse(expression, {}, replace)


class _textual_index_element(sql.ColumnElement):
    """Wrap around a sqlalchemy text() construct in such a way that
    we appear like a column-oriented SQL expression to an Index
    construct.

    The issue here is that currently the Postgresql dialect, the biggest
    recipient of functional indexes, keys all the index expressions to
    the corresponding column expressions when rendering CREATE INDEX,
    so the Index we create here needs to have a .columns collection that
    is the same length as the .expressions collection.  Ultimately
    SQLAlchemy should support text() expressions in indexes.

    See SQLAlchemy issue 3174.

    """

    __visit_name__ = "_textual_idx_element"

    def __init__(self, table, text):
        self.table = table
        self.text = text
        self.key = text.text
        self.fake_column = schema.Column(self.text.text, sqltypes.NULLTYPE)
        table.append_column(self.fake_column)

    def get_children(self):
        return [self.fake_column]


@compiles(_textual_index_element)
def _render_textual_index_column(element, compiler, **kw):
    return compiler.process(element.text, **kw)


class _literal_bindparam(_BindParamClause):
    pass


@compiles(_literal_bindparam)
def _render_literal_bindparam(element, compiler, **kw):
    return compiler.render_literal_bindparam(element, **kw)


def _get_index_expressions(idx):
    return list(idx.expressions)


def _get_index_column_names(idx):
    return [getattr(exp, "name", None) for exp in _get_index_expressions(idx)]


def _column_kwargs(col):
    if sqla_13:
        return col.kwargs
    else:
        return {}


def _get_constraint_final_name(constraint, dialect):
    if constraint.name is None:
        return None
    elif sqla_14:
        # for SQLAlchemy 1.4 we would like to have the option to expand
        # the use of "deferred" names for constraints as well as to have
        # some flexibility with "None" name and similar; make use of new
        # SQLAlchemy API to return what would be the final compiled form of
        # the name for this dialect.
        return dialect.identifier_preparer.format_constraint(
            constraint, _alembic_quote=False
        )
    else:

        # prior to SQLAlchemy 1.4, work around quoting logic to get at the
        # final compiled name without quotes.
        if hasattr(constraint.name, "quote"):
            # might be quoted_name, might be truncated_name, keep it the
            # same
            quoted_name_cls = type(constraint.name)
        else:
            quoted_name_cls = quoted_name

        new_name = quoted_name_cls(str(constraint.name), quote=False)
        constraint = constraint.__class__(name=new_name)

        if isinstance(constraint, schema.Index):
            # name should not be quoted.
            return dialect.ddl_compiler(dialect, None)._prepared_index_name(
                constraint
            )
        else:
            # name should not be quoted.
            return dialect.identifier_preparer.format_constraint(constraint)


def _constraint_is_named(constraint, dialect):
    if sqla_14:
        if constraint.name is None:
            return False
        name = dialect.identifier_preparer.format_constraint(
            constraint, _alembic_quote=False
        )
        return name is not None
    else:
        return constraint.name is not None


def _is_mariadb(mysql_dialect):
    if sqla_14:
        return mysql_dialect.is_mariadb
    else:
        return mysql_dialect.server_version_info and mysql_dialect._is_mariadb


def _mariadb_normalized_version_info(mysql_dialect):
    return mysql_dialect._mariadb_normalized_version_info


def _insert_inline(table):
    if sqla_14:
        return table.insert().inline()
    else:
        return table.insert(inline=True)


if sqla_14:
    from sqlalchemy import create_mock_engine
    from sqlalchemy import select as _select
else:
    from sqlalchemy import create_engine

    def create_mock_engine(url, executor):
        return create_engine(
            "postgresql://", strategy="mock", executor=executor
        )

    def _select(*columns):
        return sql.select(list(columns))
