from sqlalchemy import types as sqltypes
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import Column
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.sql.expression import Executable

from .base import AddColumn
from .base import alter_column
from .base import alter_table
from .base import ColumnDefault
from .base import ColumnName
from .base import ColumnNullable
from .base import ColumnType
from .base import format_column_name
from .base import format_server_default
from .base import format_table_name
from .base import format_type
from .base import RenameTable
from .impl import DefaultImpl
from .. import util
from ..util import sqla_compat


class MSSQLImpl(DefaultImpl):
    __dialect__ = "mssql"
    transactional_ddl = True
    batch_separator = "GO"

    type_synonyms = DefaultImpl.type_synonyms + ({"VARCHAR", "NVARCHAR"},)
    identity_attrs_ignore = (
        "minvalue",
        "maxvalue",
        "nominvalue",
        "nomaxvalue",
        "cycle",
        "cache",
        "order",
        "on_null",
        "order",
    )

    def __init__(self, *arg, **kw):
        super(MSSQLImpl, self).__init__(*arg, **kw)
        self.batch_separator = self.context_opts.get(
            "mssql_batch_separator", self.batch_separator
        )

    def _exec(self, construct, *args, **kw):
        result = super(MSSQLImpl, self)._exec(construct, *args, **kw)
        if self.as_sql and self.batch_separator:
            self.static_output(self.batch_separator)
        return result

    def emit_begin(self):
        self.static_output("BEGIN TRANSACTION" + self.command_terminator)

    def emit_commit(self):
        super(MSSQLImpl, self).emit_commit()
        if self.as_sql and self.batch_separator:
            self.static_output(self.batch_separator)

    def alter_column(
        self,
        table_name,
        column_name,
        nullable=None,
        server_default=False,
        name=None,
        type_=None,
        schema=None,
        existing_type=None,
        existing_server_default=None,
        existing_nullable=None,
        **kw
    ):

        if nullable is not None:
            if existing_type is None:
                if type_ is not None:
                    existing_type = type_
                    # the NULL/NOT NULL alter will handle
                    # the type alteration
                    type_ = None
                else:
                    raise util.CommandError(
                        "MS-SQL ALTER COLUMN operations "
                        "with NULL or NOT NULL require the "
                        "existing_type or a new type_ be passed."
                    )
            elif type_ is not None:
                # the NULL/NOT NULL alter will handle
                # the type alteration
                existing_type = type_
                type_ = None

        used_default = False
        if sqla_compat._server_default_is_identity(
            server_default, existing_server_default
        ) or sqla_compat._server_default_is_computed(
            server_default, existing_server_default
        ):
            used_default = True
            kw["server_default"] = server_default
            kw["existing_server_default"] = existing_server_default

        super(MSSQLImpl, self).alter_column(
            table_name,
            column_name,
            nullable=nullable,
            type_=type_,
            schema=schema,
            existing_type=existing_type,
            existing_nullable=existing_nullable,
            **kw
        )

        if server_default is not False and used_default is False:
            if existing_server_default is not False or server_default is None:
                self._exec(
                    _ExecDropConstraint(
                        table_name,
                        column_name,
                        "sys.default_constraints",
                        schema,
                    )
                )
            if server_default is not None:
                super(MSSQLImpl, self).alter_column(
                    table_name,
                    column_name,
                    schema=schema,
                    server_default=server_default,
                )

        if name is not None:
            super(MSSQLImpl, self).alter_column(
                table_name, column_name, schema=schema, name=name
            )

    def create_index(self, index):
        # this likely defaults to None if not present, so get()
        # should normally not return the default value.  being
        # defensive in any case
        mssql_include = index.kwargs.get("mssql_include", None) or ()
        for col in mssql_include:
            if col not in index.table.c:
                index.table.append_column(Column(col, sqltypes.NullType))
        self._exec(CreateIndex(index))

    def bulk_insert(self, table, rows, **kw):
        if self.as_sql:
            self._exec(
                "SET IDENTITY_INSERT %s ON"
                % self.dialect.identifier_preparer.format_table(table)
            )
            super(MSSQLImpl, self).bulk_insert(table, rows, **kw)
            self._exec(
                "SET IDENTITY_INSERT %s OFF"
                % self.dialect.identifier_preparer.format_table(table)
            )
        else:
            super(MSSQLImpl, self).bulk_insert(table, rows, **kw)

    def drop_column(self, table_name, column, schema=None, **kw):
        drop_default = kw.pop("mssql_drop_default", False)
        if drop_default:
            self._exec(
                _ExecDropConstraint(
                    table_name, column, "sys.default_constraints", schema
                )
            )
        drop_check = kw.pop("mssql_drop_check", False)
        if drop_check:
            self._exec(
                _ExecDropConstraint(
                    table_name, column, "sys.check_constraints", schema
                )
            )
        drop_fks = kw.pop("mssql_drop_foreign_key", False)
        if drop_fks:
            self._exec(_ExecDropFKConstraint(table_name, column, schema))
        super(MSSQLImpl, self).drop_column(
            table_name, column, schema=schema, **kw
        )

    def compare_server_default(
        self,
        inspector_column,
        metadata_column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):
        def clean(value):
            if value is not None:
                value = value.strip()
                while value[0] == "(" and value[-1] == ")":
                    value = value[1:-1]
            return value

        return clean(rendered_inspector_default) != clean(
            rendered_metadata_default
        )

    def _compare_identity_default(self, metadata_identity, inspector_identity):
        diff, ignored, is_alter = super(
            MSSQLImpl, self
        )._compare_identity_default(metadata_identity, inspector_identity)

        if (
            metadata_identity is None
            and inspector_identity is not None
            and not diff
            and inspector_identity.column is not None
            and inspector_identity.column.primary_key
        ):
            # mssql reflect primary keys with autoincrement as identity
            # columns. if no different attributes are present ignore them
            is_alter = False

        return diff, ignored, is_alter


class _ExecDropConstraint(Executable, ClauseElement):
    def __init__(self, tname, colname, type_, schema):
        self.tname = tname
        self.colname = colname
        self.type_ = type_
        self.schema = schema


class _ExecDropFKConstraint(Executable, ClauseElement):
    def __init__(self, tname, colname, schema):
        self.tname = tname
        self.colname = colname
        self.schema = schema


@compiles(_ExecDropConstraint, "mssql")
def _exec_drop_col_constraint(element, compiler, **kw):
    schema, tname, colname, type_ = (
        element.schema,
        element.tname,
        element.colname,
        element.type_,
    )
    # from http://www.mssqltips.com/sqlservertip/1425/\
    # working-with-default-constraints-in-sql-server/
    # TODO: needs table formatting, etc.
    return """declare @const_name varchar(256)
select @const_name = [name] from %(type)s
where parent_object_id = object_id('%(schema_dot)s%(tname)s')
and col_name(parent_object_id, parent_column_id) = '%(colname)s'
exec('alter table %(tname_quoted)s drop constraint ' + @const_name)""" % {
        "type": type_,
        "tname": tname,
        "colname": colname,
        "tname_quoted": format_table_name(compiler, tname, schema),
        "schema_dot": schema + "." if schema else "",
    }


@compiles(_ExecDropFKConstraint, "mssql")
def _exec_drop_col_fk_constraint(element, compiler, **kw):
    schema, tname, colname = element.schema, element.tname, element.colname

    return """declare @const_name varchar(256)
select @const_name = [name] from
sys.foreign_keys fk join sys.foreign_key_columns fkc
on fk.object_id=fkc.constraint_object_id
where fkc.parent_object_id = object_id('%(schema_dot)s%(tname)s')
and col_name(fkc.parent_object_id, fkc.parent_column_id) = '%(colname)s'
exec('alter table %(tname_quoted)s drop constraint ' + @const_name)""" % {
        "tname": tname,
        "colname": colname,
        "tname_quoted": format_table_name(compiler, tname, schema),
        "schema_dot": schema + "." if schema else "",
    }


@compiles(AddColumn, "mssql")
def visit_add_column(element, compiler, **kw):
    return "%s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        mssql_add_column(compiler, element.column, **kw),
    )


def mssql_add_column(compiler, column, **kw):
    return "ADD %s" % compiler.get_column_specification(column, **kw)


@compiles(ColumnNullable, "mssql")
def visit_column_nullable(element, compiler, **kw):
    return "%s %s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        format_type(compiler, element.existing_type),
        "NULL" if element.nullable else "NOT NULL",
    )


@compiles(ColumnDefault, "mssql")
def visit_column_default(element, compiler, **kw):
    # TODO: there can also be a named constraint
    # with ADD CONSTRAINT here
    return "%s ADD DEFAULT %s FOR %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_server_default(compiler, element.default),
        format_column_name(compiler, element.column_name),
    )


@compiles(ColumnName, "mssql")
def visit_rename_column(element, compiler, **kw):
    return "EXEC sp_rename '%s.%s', %s, 'COLUMN'" % (
        format_table_name(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    )


@compiles(ColumnType, "mssql")
def visit_column_type(element, compiler, **kw):
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        format_type(compiler, element.type_),
    )


@compiles(RenameTable, "mssql")
def visit_rename_table(element, compiler, **kw):
    return "EXEC sp_rename '%s', %s" % (
        format_table_name(compiler, element.table_name, element.schema),
        format_table_name(compiler, element.new_table_name, None),
    )
