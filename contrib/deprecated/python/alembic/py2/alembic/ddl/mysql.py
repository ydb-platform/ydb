import re

from sqlalchemy import schema
from sqlalchemy import types as sqltypes
from sqlalchemy.ext.compiler import compiles

from .base import alter_table
from .base import AlterColumn
from .base import ColumnDefault
from .base import ColumnName
from .base import ColumnNullable
from .base import ColumnType
from .base import format_column_name
from .base import format_server_default
from .impl import DefaultImpl
from .. import util
from ..autogenerate import compare
from ..util import sqla_compat
from ..util.sqla_compat import _is_mariadb
from ..util.sqla_compat import _is_type_bound


class MySQLImpl(DefaultImpl):
    __dialect__ = "mysql"

    transactional_ddl = False
    type_synonyms = DefaultImpl.type_synonyms + ({"BOOL", "TINYINT"},)
    type_arg_extract = [r"character set ([\w\-_]+)", r"collate ([\w\-_]+)"]

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
        autoincrement=None,
        existing_autoincrement=None,
        comment=False,
        existing_comment=None,
        **kw
    ):
        if sqla_compat._server_default_is_identity(
            server_default, existing_server_default
        ) or sqla_compat._server_default_is_computed(
            server_default, existing_server_default
        ):
            # modifying computed or identity columns is not supported
            # the default will raise
            super(MySQLImpl, self).alter_column(
                table_name,
                column_name,
                nullable=nullable,
                type_=type_,
                schema=schema,
                existing_type=existing_type,
                existing_nullable=existing_nullable,
                server_default=server_default,
                existing_server_default=existing_server_default,
                **kw
            )
        if name is not None or self._is_mysql_allowed_functional_default(
            type_ if type_ is not None else existing_type, server_default
        ):
            self._exec(
                MySQLChangeColumn(
                    table_name,
                    column_name,
                    schema=schema,
                    newname=name if name is not None else column_name,
                    nullable=nullable
                    if nullable is not None
                    else existing_nullable
                    if existing_nullable is not None
                    else True,
                    type_=type_ if type_ is not None else existing_type,
                    default=server_default
                    if server_default is not False
                    else existing_server_default,
                    autoincrement=autoincrement
                    if autoincrement is not None
                    else existing_autoincrement,
                    comment=comment
                    if comment is not False
                    else existing_comment,
                )
            )
        elif (
            nullable is not None
            or type_ is not None
            or autoincrement is not None
            or comment is not False
        ):
            self._exec(
                MySQLModifyColumn(
                    table_name,
                    column_name,
                    schema=schema,
                    newname=name if name is not None else column_name,
                    nullable=nullable
                    if nullable is not None
                    else existing_nullable
                    if existing_nullable is not None
                    else True,
                    type_=type_ if type_ is not None else existing_type,
                    default=server_default
                    if server_default is not False
                    else existing_server_default,
                    autoincrement=autoincrement
                    if autoincrement is not None
                    else existing_autoincrement,
                    comment=comment
                    if comment is not False
                    else existing_comment,
                )
            )
        elif server_default is not False:
            self._exec(
                MySQLAlterDefault(
                    table_name, column_name, server_default, schema=schema
                )
            )

    def drop_constraint(self, const):
        if isinstance(const, schema.CheckConstraint) and _is_type_bound(const):
            return

        super(MySQLImpl, self).drop_constraint(const)

    def _is_mysql_allowed_functional_default(self, type_, server_default):
        return (
            type_ is not None
            and type_._type_affinity is sqltypes.DateTime
            and server_default is not None
        )

    def compare_server_default(
        self,
        inspector_column,
        metadata_column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):
        # partially a workaround for SQLAlchemy issue #3023; if the
        # column were created without "NOT NULL", MySQL may have added
        # an implicit default of '0' which we need to skip
        # TODO: this is not really covered anymore ?
        if (
            metadata_column.type._type_affinity is sqltypes.Integer
            and inspector_column.primary_key
            and not inspector_column.autoincrement
            and not rendered_metadata_default
            and rendered_inspector_default == "'0'"
        ):
            return False
        elif inspector_column.type._type_affinity is sqltypes.Integer:
            rendered_inspector_default = (
                re.sub(r"^'|'$", "", rendered_inspector_default)
                if rendered_inspector_default is not None
                else None
            )
            return rendered_inspector_default != rendered_metadata_default
        elif rendered_inspector_default and rendered_metadata_default:
            # adjust for "function()" vs. "FUNCTION" as can occur particularly
            # for the CURRENT_TIMESTAMP function on newer MariaDB versions

            # SQLAlchemy MySQL dialect bundles ON UPDATE into the server
            # default; adjust for this possibly being present.
            onupdate_ins = re.match(
                r"(.*) (on update.*?)(?:\(\))?$",
                rendered_inspector_default.lower(),
            )
            onupdate_met = re.match(
                r"(.*) (on update.*?)(?:\(\))?$",
                rendered_metadata_default.lower(),
            )

            if onupdate_ins:
                if not onupdate_met:
                    return True
                elif onupdate_ins.group(2) != onupdate_met.group(2):
                    return True

                rendered_inspector_default = onupdate_ins.group(1)
                rendered_metadata_default = onupdate_met.group(1)

            return re.sub(
                r"(.*?)(?:\(\))?$", r"\1", rendered_inspector_default.lower()
            ) != re.sub(
                r"(.*?)(?:\(\))?$", r"\1", rendered_metadata_default.lower()
            )
        else:
            return rendered_inspector_default != rendered_metadata_default

    def correct_for_autogen_constraints(
        self,
        conn_unique_constraints,
        conn_indexes,
        metadata_unique_constraints,
        metadata_indexes,
    ):

        # TODO: if SQLA 1.0, make use of "duplicates_index"
        # metadata
        removed = set()
        for idx in list(conn_indexes):
            if idx.unique:
                continue
            # MySQL puts implicit indexes on FK columns, even if
            # composite and even if MyISAM, so can't check this too easily.
            # the name of the index may be the column name or it may
            # be the name of the FK constraint.
            for col in idx.columns:
                if idx.name == col.name:
                    conn_indexes.remove(idx)
                    removed.add(idx.name)
                    break
                for fk in col.foreign_keys:
                    if fk.name == idx.name:
                        conn_indexes.remove(idx)
                        removed.add(idx.name)
                        break
                if idx.name in removed:
                    break

        # then remove indexes from the "metadata_indexes"
        # that we've removed from reflected, otherwise they come out
        # as adds (see #202)
        for idx in list(metadata_indexes):
            if idx.name in removed:
                metadata_indexes.remove(idx)

    def correct_for_autogen_foreignkeys(self, conn_fks, metadata_fks):
        conn_fk_by_sig = dict(
            (compare._fk_constraint_sig(fk).sig, fk) for fk in conn_fks
        )
        metadata_fk_by_sig = dict(
            (compare._fk_constraint_sig(fk).sig, fk) for fk in metadata_fks
        )

        for sig in set(conn_fk_by_sig).intersection(metadata_fk_by_sig):
            mdfk = metadata_fk_by_sig[sig]
            cnfk = conn_fk_by_sig[sig]
            # MySQL considers RESTRICT to be the default and doesn't
            # report on it.  if the model has explicit RESTRICT and
            # the conn FK has None, set it to RESTRICT
            if (
                mdfk.ondelete is not None
                and mdfk.ondelete.lower() == "restrict"
                and cnfk.ondelete is None
            ):
                cnfk.ondelete = "RESTRICT"
            if (
                mdfk.onupdate is not None
                and mdfk.onupdate.lower() == "restrict"
                and cnfk.onupdate is None
            ):
                cnfk.onupdate = "RESTRICT"


class MariaDBImpl(MySQLImpl):
    __dialect__ = "mariadb"


class MySQLAlterDefault(AlterColumn):
    def __init__(self, name, column_name, default, schema=None):
        super(AlterColumn, self).__init__(name, schema=schema)
        self.column_name = column_name
        self.default = default


class MySQLChangeColumn(AlterColumn):
    def __init__(
        self,
        name,
        column_name,
        schema=None,
        newname=None,
        type_=None,
        nullable=None,
        default=False,
        autoincrement=None,
        comment=False,
    ):
        super(AlterColumn, self).__init__(name, schema=schema)
        self.column_name = column_name
        self.nullable = nullable
        self.newname = newname
        self.default = default
        self.autoincrement = autoincrement
        self.comment = comment
        if type_ is None:
            raise util.CommandError(
                "All MySQL CHANGE/MODIFY COLUMN operations "
                "require the existing type."
            )

        self.type_ = sqltypes.to_instance(type_)


class MySQLModifyColumn(MySQLChangeColumn):
    pass


@compiles(ColumnNullable, "mysql", "mariadb")
@compiles(ColumnName, "mysql", "mariadb")
@compiles(ColumnDefault, "mysql", "mariadb")
@compiles(ColumnType, "mysql", "mariadb")
def _mysql_doesnt_support_individual(element, compiler, **kw):
    raise NotImplementedError(
        "Individual alter column constructs not supported by MySQL"
    )


@compiles(MySQLAlterDefault, "mysql", "mariadb")
def _mysql_alter_default(element, compiler, **kw):
    return "%s ALTER COLUMN %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        "SET DEFAULT %s" % format_server_default(compiler, element.default)
        if element.default is not None
        else "DROP DEFAULT",
    )


@compiles(MySQLModifyColumn, "mysql", "mariadb")
def _mysql_modify_column(element, compiler, **kw):
    return "%s MODIFY %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        _mysql_colspec(
            compiler,
            nullable=element.nullable,
            server_default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
        ),
    )


@compiles(MySQLChangeColumn, "mysql", "mariadb")
def _mysql_change_column(element, compiler, **kw):
    return "%s CHANGE %s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
        _mysql_colspec(
            compiler,
            nullable=element.nullable,
            server_default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
        ),
    )


def _mysql_colspec(
    compiler, nullable, server_default, type_, autoincrement, comment
):
    spec = "%s %s" % (
        compiler.dialect.type_compiler.process(type_),
        "NULL" if nullable else "NOT NULL",
    )
    if autoincrement:
        spec += " AUTO_INCREMENT"
    if server_default is not False and server_default is not None:
        spec += " DEFAULT %s" % format_server_default(compiler, server_default)
    if comment:
        spec += " COMMENT %s" % compiler.sql_compiler.render_literal_value(
            comment, sqltypes.String()
        )

    return spec


@compiles(schema.DropConstraint, "mysql", "mariadb")
def _mysql_drop_constraint(element, compiler, **kw):
    """Redefine SQLAlchemy's drop constraint to
    raise errors for invalid constraint type."""

    constraint = element.element
    if isinstance(
        constraint,
        (
            schema.ForeignKeyConstraint,
            schema.PrimaryKeyConstraint,
            schema.UniqueConstraint,
        ),
    ):
        return compiler.visit_drop_constraint(element, **kw)
    elif isinstance(constraint, schema.CheckConstraint):
        # note that SQLAlchemy as of 1.2 does not yet support
        # DROP CONSTRAINT for MySQL/MariaDB, so we implement fully
        # here.
        if _is_mariadb(compiler.dialect):
            return "ALTER TABLE %s DROP CONSTRAINT %s" % (
                compiler.preparer.format_table(constraint.table),
                compiler.preparer.format_constraint(constraint),
            )
        else:
            return "ALTER TABLE %s DROP CHECK %s" % (
                compiler.preparer.format_table(constraint.table),
                compiler.preparer.format_constraint(constraint),
            )
    else:
        raise NotImplementedError(
            "No generic 'DROP CONSTRAINT' in MySQL - "
            "please specify constraint type"
        )
