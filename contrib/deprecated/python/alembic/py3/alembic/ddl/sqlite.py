import re

from sqlalchemy import cast
from sqlalchemy import JSON

from .impl import DefaultImpl
from .. import util


class SQLiteImpl(DefaultImpl):
    __dialect__ = "sqlite"

    transactional_ddl = False
    """SQLite supports transactional DDL, but pysqlite does not:
    see: http://bugs.python.org/issue10740
    """

    def requires_recreate_in_batch(self, batch_op):
        """Return True if the given :class:`.BatchOperationsImpl`
        would need the table to be recreated and copied in order to
        proceed.

        Normally, only returns True on SQLite when operations other
        than add_column are present.

        """
        for op in batch_op.batch:
            if op[0] not in ("add_column", "create_index", "drop_index"):
                return True
        else:
            return False

    def add_constraint(self, const):
        # attempt to distinguish between an
        # auto-gen constraint and an explicit one
        if const._create_rule is None:
            raise NotImplementedError(
                "No support for ALTER of constraints in SQLite dialect"
                "Please refer to the batch mode feature which allows for "
                "SQLite migrations using a copy-and-move strategy."
            )
        elif const._create_rule(self):
            util.warn(
                "Skipping unsupported ALTER for "
                "creation of implicit constraint"
                "Please refer to the batch mode feature which allows for "
                "SQLite migrations using a copy-and-move strategy."
            )

    def drop_constraint(self, const):
        if const._create_rule is None:
            raise NotImplementedError(
                "No support for ALTER of constraints in SQLite dialect"
                "Please refer to the batch mode feature which allows for "
                "SQLite migrations using a copy-and-move strategy."
            )

    def compare_server_default(
        self,
        inspector_column,
        metadata_column,
        rendered_metadata_default,
        rendered_inspector_default,
    ):

        if rendered_metadata_default is not None:
            rendered_metadata_default = re.sub(
                r"^\((.+)\)$", r"\1", rendered_metadata_default
            )

            rendered_metadata_default = re.sub(
                r"^\"?'(.+)'\"?$", r"\1", rendered_metadata_default
            )

        if rendered_inspector_default is not None:
            rendered_inspector_default = re.sub(
                r"^\"?'(.+)'\"?$", r"\1", rendered_inspector_default
            )

        return rendered_inspector_default != rendered_metadata_default

    def _guess_if_default_is_unparenthesized_sql_expr(self, expr):
        """Determine if a server default is a SQL expression or a constant.

        There are too many assertions that expect server defaults to round-trip
        identically without parenthesis added so we will add parens only in
        very specific cases.

        """
        if not expr:
            return False
        elif re.match(r"^[0-9\.]$", expr):
            return False
        elif re.match(r"^'.+'$", expr):
            return False
        elif re.match(r"^\(.+\)$", expr):
            return False
        else:
            return True

    def autogen_column_reflect(self, inspector, table, column_info):
        # SQLite expression defaults require parenthesis when sent
        # as DDL
        if self._guess_if_default_is_unparenthesized_sql_expr(
            column_info.get("default", None)
        ):
            column_info["default"] = "(%s)" % (column_info["default"],)

    def render_ddl_sql_expr(self, expr, is_server_default=False, **kw):
        # SQLite expression defaults require parenthesis when sent
        # as DDL
        str_expr = super(SQLiteImpl, self).render_ddl_sql_expr(
            expr, is_server_default=is_server_default, **kw
        )

        if (
            is_server_default
            and self._guess_if_default_is_unparenthesized_sql_expr(str_expr)
        ):
            str_expr = "(%s)" % (str_expr,)
        return str_expr

    def cast_for_batch_migrate(self, existing, existing_transfer, new_type):
        if (
            existing.type._type_affinity is not new_type._type_affinity
            and not isinstance(new_type, JSON)
        ):
            existing_transfer["expr"] = cast(
                existing_transfer["expr"], new_type
            )


# @compiles(AddColumn, 'sqlite')
# def visit_add_column(element, compiler, **kw):
#    return "%s %s" % (
#        alter_table(compiler, element.table_name, element.schema),
#        add_column(compiler, element.column, **kw)
#    )


# def add_column(compiler, column, **kw):
#    text = "ADD COLUMN %s" % compiler.get_column_specification(column, **kw)
# need to modify SQLAlchemy so that the CHECK associated with a Boolean
# or Enum gets placed as part of the column constraints, not the Table
# see ticket 98
#    for const in column.constraints:
#        text += compiler.process(AddConstraint(const))
#    return text
