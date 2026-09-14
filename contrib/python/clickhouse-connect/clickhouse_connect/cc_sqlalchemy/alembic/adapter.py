from collections.abc import Mapping
from typing import Any

from alembic.autogenerate import render
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.compare import comparators
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError, DispatchPriority, PriorityDispatchResult

from clickhouse_connect.cc_sqlalchemy.alembic.impl import ClickHouseImpl
from clickhouse_connect.cc_sqlalchemy.alembic.operations import (
    ClickHouseIndex,
    ClickHouseProjection,
    _AddClickHouseIndexesOp,
    _AddClickHouseIndexOp,
    _AddClickHouseProjectionOp,
    _AddClickHouseProjectionsOp,
    _CreateClickHouseDictionaryOp,
    _CreateClickHouseMaterializedViewOp,
    _DropClickHouseDictionaryOp,
    _DropClickHouseIndexesOp,
    _DropClickHouseIndexOp,
    _DropClickHouseMaterializedViewOp,
    _DropClickHouseProjectionOp,
    _DropClickHouseProjectionsOp,
    _MaterializeClickHouseIndexOp,
    _MaterializeClickHouseProjectionOp,
    _ModifyClickHouseTableSettingsOp,
    _ReloadClickHouseDictionaryOp,
    _ResetClickHouseTableSettingsOp,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper

__all__ = ["clickhouse_writer", "include_object", "patch_alembic_version"]


@Operations.register_operation("add_column")
class _ClickHouseAddColumnOp(ops.AddColumnOp):
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


def patch_alembic_version(context: MigrationContext) -> MigrationContext:
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


def clickhouse_writer(context: MigrationContext, revision: Any, directives: list[Any]) -> None:
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


def _is_clickhouse_autogen(autogen_context: AutogenContext) -> bool:
    """True only when the active migration context targets the ClickHouse dialect."""
    migration_context = getattr(autogen_context, "migration_context", None)
    return isinstance(getattr(migration_context, "impl", None), ClickHouseImpl)


def _render_clickhouse_column(column, autogen_context: AutogenContext) -> str:
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


# Alembic renderers have no dialect qualifier, so replace=True overrides rendering
# process-wide. Capture each built-in renderer before replacing it and delegate to it for
# non-ClickHouse dialects so autogenerate stays correct for other databases (#832). Held on
# the module so importlib.reload does not re-capture one of our own renderers and recurse.
_DEFAULT_RENDERERS = globals().get("_DEFAULT_RENDERERS") or {
    op: render.renderers.dispatch(op) for op in (ops.CreateTableOp, ops.AddColumnOp, ops.DropTableOp)
}


@render.renderers.dispatch_for(ops.CreateTableOp, replace=True)
def _render_create_table(autogen_context: AutogenContext, op: ops.CreateTableOp) -> str:
    if not _is_clickhouse_autogen(autogen_context):
        return _DEFAULT_RENDERERS[ops.CreateTableOp](autogen_context, op)
    table = op.to_table()

    args = [column for column in [_render_clickhouse_column(column, autogen_context) for column in table.columns] if column] + sorted(
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
def _render_add_column(autogen_context: AutogenContext, op: ops.AddColumnOp) -> str:
    if not _is_clickhouse_autogen(autogen_context):
        return _DEFAULT_RENDERERS[ops.AddColumnOp](autogen_context, op)
    schema, table_name, column, if_not_exists = op.schema, op.table_name, op.column, op.if_not_exists
    prefix = render._alembic_autogenerate_prefix(autogen_context)
    rendered_column = _render_clickhouse_column(column, autogen_context)
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
def _render_drop_table(autogen_context: AutogenContext, op: ops.DropTableOp) -> str:
    if not _is_clickhouse_autogen(autogen_context):
        return _DEFAULT_RENDERERS[ops.DropTableOp](autogen_context, op)
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


def _render_literal(value: object) -> str:
    if isinstance(value, Mapping):
        value = dict(value)
    return repr(value)


def _render_kwargs(kwargs: list[tuple[str, object]]) -> list[str]:
    return [f"{name}={_render_literal(value)}" for name, value in kwargs]


def _render_op_call(autogen_context: AutogenContext, name: str, args: list[str], kwargs: list[tuple[str, object]]) -> str:
    prefix = render._alembic_autogenerate_prefix(autogen_context)
    params = args + _render_kwargs(kwargs)
    return f"{prefix}{name}({', '.join(params)})"


def _optional_kwargs(*items: tuple[str, object, object]) -> list[tuple[str, object]]:
    return [(name, value) for name, value, default in items if value != default]


def _render_clickhouse_index(autogen_context: AutogenContext, index: ClickHouseIndex) -> str:
    autogen_context.imports.add("from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseIndex")
    kwargs = _optional_kwargs(
        ("granularity", index.granularity, None),
        ("if_not_exists", index.if_not_exists, False),
        ("first", index.first, False),
        ("after_index", index.after_index, None),
    )
    params = [repr(index.name), repr(index.expression), repr(index.type_)] + _render_kwargs(kwargs)
    return f"ClickHouseIndex({', '.join(params)})"


def _render_clickhouse_projection(autogen_context: AutogenContext, projection: ClickHouseProjection) -> str:
    autogen_context.imports.add("from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseProjection")
    kwargs = _optional_kwargs(
        ("if_not_exists", projection.if_not_exists, False),
        ("first", projection.first, False),
        ("after_projection", projection.after_projection, None),
    )
    params = [repr(projection.name), repr(projection.select)] + _render_kwargs(kwargs)
    return f"ClickHouseProjection({', '.join(params)})"


@render.renderers.dispatch_for(_AddClickHouseIndexOp)
def _render_add_clickhouse_index(autogen_context: AutogenContext, op: _AddClickHouseIndexOp) -> str:
    return _render_op_call(
        autogen_context,
        "add_clickhouse_index",
        [repr(op.table_name), repr(op.name), repr(op.expression), repr(op.type_)],
        _optional_kwargs(
            ("granularity", op.granularity, None),
            ("if_not_exists", op.if_not_exists, False),
            ("first", op.first, False),
            ("after_index", op.after_index, None),
            ("schema", op.schema, None),
            ("clickhouse_settings", op.clickhouse_settings, None),
        ),
    )


@render.renderers.dispatch_for(_AddClickHouseIndexesOp)
def _render_add_clickhouse_indexes(autogen_context: AutogenContext, op: _AddClickHouseIndexesOp) -> str:
    indexes = "[" + ", ".join(_render_clickhouse_index(autogen_context, index) for index in op.indexes) + "]"
    return _render_op_call(
        autogen_context,
        "add_clickhouse_indexes",
        [repr(op.table_name), indexes],
        _optional_kwargs(("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)),
    )


@render.renderers.dispatch_for(_DropClickHouseIndexOp)
def _render_drop_clickhouse_index(autogen_context: AutogenContext, op: _DropClickHouseIndexOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_index",
        [repr(op.table_name), repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_DropClickHouseIndexesOp)
def _render_drop_clickhouse_indexes(autogen_context: AutogenContext, op: _DropClickHouseIndexesOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_indexes",
        [repr(op.table_name), repr(list(op.names))],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_MaterializeClickHouseIndexOp)
def _render_materialize_clickhouse_index(autogen_context: AutogenContext, op: _MaterializeClickHouseIndexOp) -> str:
    return _render_op_call(
        autogen_context,
        "materialize_clickhouse_index",
        [repr(op.table_name), repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False),
            ("partition", op.partition, None),
            ("schema", op.schema, None),
            ("clickhouse_settings", op.clickhouse_settings, None),
        ),
    )


@render.renderers.dispatch_for(_AddClickHouseProjectionOp)
def _render_add_clickhouse_projection(autogen_context: AutogenContext, op: _AddClickHouseProjectionOp) -> str:
    return _render_op_call(
        autogen_context,
        "add_clickhouse_projection",
        [repr(op.table_name), repr(op.name), repr(op.select)],
        _optional_kwargs(
            ("if_not_exists", op.if_not_exists, False),
            ("first", op.first, False),
            ("after_projection", op.after_projection, None),
            ("schema", op.schema, None),
            ("clickhouse_settings", op.clickhouse_settings, None),
        ),
    )


@render.renderers.dispatch_for(_AddClickHouseProjectionsOp)
def _render_add_clickhouse_projections(autogen_context: AutogenContext, op: _AddClickHouseProjectionsOp) -> str:
    projections = "[" + ", ".join(_render_clickhouse_projection(autogen_context, projection) for projection in op.projections) + "]"
    return _render_op_call(
        autogen_context,
        "add_clickhouse_projections",
        [repr(op.table_name), projections],
        _optional_kwargs(("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)),
    )


@render.renderers.dispatch_for(_DropClickHouseProjectionOp)
def _render_drop_clickhouse_projection(autogen_context: AutogenContext, op: _DropClickHouseProjectionOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_projection",
        [repr(op.table_name), repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_DropClickHouseProjectionsOp)
def _render_drop_clickhouse_projections(autogen_context: AutogenContext, op: _DropClickHouseProjectionsOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_projections",
        [repr(op.table_name), repr(list(op.names))],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_MaterializeClickHouseProjectionOp)
def _render_materialize_clickhouse_projection(autogen_context: AutogenContext, op: _MaterializeClickHouseProjectionOp) -> str:
    return _render_op_call(
        autogen_context,
        "materialize_clickhouse_projection",
        [repr(op.table_name), repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False),
            ("partition", op.partition, None),
            ("schema", op.schema, None),
            ("clickhouse_settings", op.clickhouse_settings, None),
        ),
    )


@render.renderers.dispatch_for(_ModifyClickHouseTableSettingsOp)
def _render_modify_clickhouse_table_settings(autogen_context: AutogenContext, op: _ModifyClickHouseTableSettingsOp) -> str:
    return _render_op_call(
        autogen_context,
        "modify_clickhouse_table_settings",
        [repr(op.table_name), _render_literal(op.settings)],
        _optional_kwargs(("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)),
    )


@render.renderers.dispatch_for(_ResetClickHouseTableSettingsOp)
def _render_reset_clickhouse_table_settings(autogen_context: AutogenContext, op: _ResetClickHouseTableSettingsOp) -> str:
    return _render_op_call(
        autogen_context,
        "reset_clickhouse_table_settings",
        [repr(op.table_name), repr(list(op.names))],
        _optional_kwargs(("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)),
    )


@render.renderers.dispatch_for(_CreateClickHouseMaterializedViewOp)
def _render_create_clickhouse_materialized_view(autogen_context: AutogenContext, op: _CreateClickHouseMaterializedViewOp) -> str:
    return _render_op_call(
        autogen_context,
        "create_clickhouse_materialized_view",
        [repr(op.name), repr(op.to_table), repr(op.select)],
        _optional_kwargs(("if_not_exists", op.if_not_exists, False), ("schema", op.schema, None), ("to_schema", op.to_schema, None)),
    )


@render.renderers.dispatch_for(_DropClickHouseMaterializedViewOp)
def _render_drop_clickhouse_materialized_view(autogen_context: AutogenContext, op: _DropClickHouseMaterializedViewOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_materialized_view",
        [repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_CreateClickHouseDictionaryOp)
def _render_create_clickhouse_dictionary(autogen_context: AutogenContext, op: _CreateClickHouseDictionaryOp) -> str:
    columns = "[" + ", ".join(_render_clickhouse_column(column, autogen_context) for column in op.columns) + "]"
    return _render_op_call(
        autogen_context,
        "create_clickhouse_dictionary",
        [repr(op.name), columns],
        _optional_kwargs(
            ("primary_key", op.primary_key, None),
            ("source", op.source, None),
            ("layout", op.layout, None),
            ("lifetime", op.lifetime, None),
            ("if_not_exists", op.if_not_exists, False),
            ("schema", op.schema, None),
            ("comment", op.comment, None),
            ("clickhouse_settings", op.clickhouse_settings, None),
        ),
    )


@render.renderers.dispatch_for(_DropClickHouseDictionaryOp)
def _render_drop_clickhouse_dictionary(autogen_context: AutogenContext, op: _DropClickHouseDictionaryOp) -> str:
    return _render_op_call(
        autogen_context,
        "drop_clickhouse_dictionary",
        [repr(op.name)],
        _optional_kwargs(
            ("if_exists", op.if_exists, False), ("schema", op.schema, None), ("clickhouse_settings", op.clickhouse_settings, None)
        ),
    )


@render.renderers.dispatch_for(_ReloadClickHouseDictionaryOp)
def _render_reload_clickhouse_dictionary(autogen_context: AutogenContext, op: _ReloadClickHouseDictionaryOp) -> str:
    return _render_op_call(
        autogen_context,
        "reload_clickhouse_dictionary",
        [repr(op.name)],
        _optional_kwargs(("schema", op.schema, None)),
    )


def include_object(object_: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
    """
    Standard filter for ClickHouse system tables and internal objects.
    """
    if type_ == "index":
        if reflected:
            return False
        raise CommandError(
            "ClickHouse data skipping indexes cannot be created with SQLAlchemy Index, "
            "Column(index=True), or autogenerate. Use op.add_clickhouse_index "
            "and op.drop_clickhouse_index for ClickHouse data skipping indexes, "
            "or op.execute for custom DDL."
        )

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
def _compare_nullable(context, alter_column_op, schema, table_name, column_name, inspector_column, metadata_column):
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
