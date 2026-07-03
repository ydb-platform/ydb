from alembic.autogenerate import render
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.compare import comparators
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import DispatchPriority, PriorityDispatchResult

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper


@Operations.register_operation("add_column")
class ClickHouseAddColumnOp(ops.AddColumnOp):
    """Re-registers op.add_column with a **kw signature."""

    @classmethod
    def add_column(cls, operations, table_name, column, *, schema=None, if_not_exists=None, **kw):
        return operations.invoke(
            ops.AddColumnOp(
                table_name,
                column,
                schema=schema,
                if_not_exists=if_not_exists,
                **kw,
            )
        )


def patch_alembic_version(context: MigrationContext):
    """
    Compatibility hook for existing migration environments.

    Version-table behavior now lives on ClickHouseImpl and no longer requires
    monkey-patching the Alembic context.
    """
    return context


def _add_common_imports(directive):
    directive.imports.add("from clickhouse_connect import cc_sqlalchemy")
    directive.imports.add("from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import *  # noqa: F401,F403")
    directive.imports.add("from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import *  # noqa: F401,F403")


def clickhouse_writer(context: MigrationContext, revision, directives):
    """
    A processing hook for autogeneration.

    Ensures that generated migration scripts include necessary imports
    and that ClickHouse-specific constructs like Engines are preserved.
    """
    for directive in directives:
        if directive.upgrade_ops and not directive.upgrade_ops.is_empty():
            _add_common_imports(directive)

        if directive.downgrade_ops and not directive.downgrade_ops.is_empty():
            _add_common_imports(directive)


def render_clickhouse_column(column, autogen_context: AutogenContext) -> str:
    rendered = render._user_defined_render("column", column, autogen_context)
    if rendered is not False:
        return rendered

    args = []
    opts = []

    if column.server_default:
        rendered_default = render._render_server_default(column.server_default, autogen_context)
        if rendered_default:
            if render._should_render_server_default_positionally(column.server_default):
                args.append(rendered_default)
            else:
                opts.append(("server_default", rendered_default))

    if column.autoincrement is not None and column.autoincrement != render.sqla_compat.AUTOINCREMENT_DEFAULT:
        opts.append(("autoincrement", column.autoincrement))

    explicit_nullable = ClickHouseDDLHelper.explicit_column_nullable(column)
    if column.nullable is not None and explicit_nullable is not None:
        opts.append(("nullable", column.nullable))

    if column.system:
        opts.append(("system", column.system))

    if column.comment:
        opts.append(("comment", repr(column.comment)))

    return "{prefix}Column({name!r}, {type}, {args}{kwargs})".format(
        prefix=render._sqlalchemy_autogenerate_prefix(autogen_context),
        name=render._ident(column.name),
        type=render._repr_type(column.type, autogen_context),
        args=", ".join(str(arg) for arg in args) + ", " if args else "",
        kwargs=", ".join(
            [f"{key}={value}" for key, value in opts]
            + [f"{key}={render._render_potential_expr(value, autogen_context)}" for key, value in column.kwargs.items()]
        ),
    )


@render.renderers.dispatch_for(ops.CreateTableOp, replace=True)
def render_create_table(autogen_context: AutogenContext, op: ops.CreateTableOp) -> str:
    table = op.to_table()

    args = [column for column in [render_clickhouse_column(column, autogen_context) for column in table.columns] if column] + sorted(
        [
            constraint
            for constraint in [render._render_constraint(cons, autogen_context, op._namespace_metadata) for cons in table.constraints]
            if constraint is not None
        ]
    )

    if len(args) > render.MAX_PYTHON_ARGS:
        args_sql = "*[" + ",\n".join(args) + "]"
    else:
        args_sql = ",\n".join(args)

    prefix = render._alembic_autogenerate_prefix(autogen_context)
    rendered = f"{prefix}create_table({render._ident(op.table_name)!r},\n{args_sql}"
    if op.schema:
        rendered += f",\nschema={render._ident(op.schema)!r}"

    if table.comment:
        rendered += f",\ncomment={render._ident(table.comment)!r}"

    if table.info:
        rendered += f",\ninfo={table.info!r}"

    for key in sorted(op.kw):
        rendered += f",\n{key.replace(' ', '_')}={op.kw[key]!r}"

    if op.if_not_exists is not None:
        rendered += f",\nif_not_exists={bool(op.if_not_exists)!r}"

    rendered += "\n)"
    return rendered


@render.renderers.dispatch_for(ops.AddColumnOp, replace=True)
def render_add_column(autogen_context: AutogenContext, op: ops.AddColumnOp) -> str:
    schema, table_name, column, if_not_exists = op.schema, op.table_name, op.column, op.if_not_exists
    prefix = render._alembic_autogenerate_prefix(autogen_context)
    rendered_column = render_clickhouse_column(column, autogen_context)
    if autogen_context._has_batch:
        return f"{prefix}add_column({rendered_column})"
    rendered = f"{prefix}add_column({table_name!r}, {rendered_column}"
    if schema:
        rendered += f", schema={schema!r}"
    if if_not_exists is not None:
        rendered += f", if_not_exists={if_not_exists!r}"
    for key in sorted(op.kw):
        rendered += f", {key}={op.kw[key]!r}"
    return rendered + ")"


@render.renderers.dispatch_for(ops.DropTableOp, replace=True)
def render_drop_table(autogen_context: AutogenContext, op: ops.DropTableOp) -> str:
    prefix = render._alembic_autogenerate_prefix(autogen_context)
    rendered = f"{prefix}drop_table({render._ident(op.table_name)!r}"
    arguments = []
    if op.schema:
        arguments.append(f"schema={render._ident(op.schema)!r}")
    if op.if_exists is not None:
        arguments.append(f"if_exists={bool(op.if_exists)!r}")
    for key in sorted(op.table_kw):
        arguments.append(f"{key.replace(' ', '_')}={op.table_kw[key]!r}")
    if arguments:
        rendered += ",\n" + ",\n".join(arguments)
    rendered += ")"
    return rendered


def include_object(object_, name, type_, reflected, compare_to):
    """
    Standard filter for ClickHouse system tables and internal objects.
    """
    # Guard against None name which can happen in some Alembic versions/contexts
    if not name:
        return True

    if type_ == "table":
        if name == "alembic_version":
            return False
        # Ignore system tables
        if object_.schema == "system":
            return False
        # Ignore internal tables (Materialized View storage)
        if name.startswith(".inner"):
            return False

    return True


@comparators.dispatch_for("column", qualifier="clickhousedb", priority=DispatchPriority.FIRST, subgroup="nullable")
def compare_nullable(context, alter_column_op, schema, table_name, column_name, inspector_column, metadata_column):
    inspector_type = inspector_column.type
    metadata_type = metadata_column.type
    if not isinstance(inspector_type, ChSqlaType) or not isinstance(metadata_type, ChSqlaType):
        return PriorityDispatchResult.CONTINUE

    inspector_nullable = inspector_type.nullable
    explicit_nullable = ClickHouseDDLHelper.explicit_column_nullable(metadata_column)
    if explicit_nullable is None and not metadata_type.nullable:
        metadata_nullable = inspector_nullable
    else:
        metadata_nullable = ClickHouseDDLHelper.column_nullable(metadata_column)
    alter_column_op.existing_nullable = inspector_nullable
    if inspector_nullable != metadata_nullable:
        alter_column_op.modify_nullable = metadata_nullable
    return PriorityDispatchResult.STOP
