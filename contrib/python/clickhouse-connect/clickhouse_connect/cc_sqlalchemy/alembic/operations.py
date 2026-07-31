from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from alembic.operations import MigrateOperation, Operations
from sqlalchemy import Column

from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import column_specification, render_settings
from clickhouse_connect.driver.binding import format_str, quote_identifier

__all__ = ["ClickHouseIndex", "ClickHouseProjection"]


@dataclass(frozen=True)
class ClickHouseIndex:
    """A data skipping index definition. expression and type_ are raw SQL passthrough."""

    name: str
    expression: str
    type_: str
    granularity: int | None = None
    if_not_exists: bool = False
    first: bool = False
    after_index: str | None = None

    def __post_init__(self) -> None:
        _validate_position(self.first, self.after_index, "ClickHouseIndex")


@dataclass(frozen=True)
class ClickHouseProjection:
    """A projection definition. select is the raw SQL body inside the parens."""

    name: str
    select: str
    if_not_exists: bool = False
    first: bool = False
    after_projection: str | None = None

    def __post_init__(self) -> None:
        _validate_position(self.first, self.after_projection, "ClickHouseProjection")


def _validate_position(first: bool, after_name: str | None, owner: str) -> None:
    if first and after_name is not None:
        raise ValueError(f"{owner} cannot specify both first and after placement")


def _exec_sql(operations: Operations, sql: str) -> None:
    impl = operations.get_context().impl
    if impl.as_sql:
        impl.static_output(sql.strip() + impl.command_terminator)
        return

    connection = impl.connection
    assert connection is not None
    connection.exec_driver_sql(sql)


def _settings_suffix(clickhouse_settings: Mapping[str, Any] | None) -> str:
    rendered = render_settings(clickhouse_settings)
    return f" SETTINGS {rendered}" if rendered else ""


def _render_column_list(operations: Operations, columns: Sequence[Column]) -> str:
    dialect = operations.get_context().dialect
    return ", ".join(column_specification(dialect, column) for column in columns)


def _render_add_index(index: ClickHouseIndex) -> str:
    parts = ["ADD INDEX"]
    if index.if_not_exists:
        parts.append("IF NOT EXISTS")
    parts.append(quote_identifier(index.name))
    parts.append(index.expression)
    parts.append("TYPE")
    parts.append(index.type_)
    if index.granularity is not None:
        parts.append(f"GRANULARITY {index.granularity}")
    if index.first:
        parts.append("FIRST")
    elif index.after_index is not None:
        parts.append(f"AFTER {quote_identifier(index.after_index)}")
    return " ".join(parts)


def _render_add_projection(projection: ClickHouseProjection) -> str:
    parts = ["ADD PROJECTION"]
    if projection.if_not_exists:
        parts.append("IF NOT EXISTS")
    parts.append(quote_identifier(projection.name))
    parts.append(f"({projection.select})")
    if projection.first:
        parts.append("FIRST")
    elif projection.after_projection is not None:
        parts.append(f"AFTER {quote_identifier(projection.after_projection)}")
    return " ".join(parts)


@Operations.register_operation("add_clickhouse_index")
class _AddClickHouseIndexOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        expression: str,
        type_: str,
        *,
        granularity: int | None = None,
        if_not_exists: bool = False,
        first: bool = False,
        after_index: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        _validate_position(first, after_index, self.__class__.__name__)
        self.table_name = table_name
        self.name = name
        self.expression = expression
        self.type_ = type_
        self.granularity = granularity
        self.if_not_exists = if_not_exists
        self.first = first
        self.after_index = after_index
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def add_clickhouse_index(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        expression: str,
        type_: str,
        granularity: int | None = None,
        if_not_exists: bool = False,
        first: bool = False,
        after_index: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... ADD INDEX.

        expression and type_ are raw SQL passthrough. Metadata-only: no mutation is
        scheduled and existing parts are not backfilled, so no sync setting applies.
        Call materialize_clickhouse_index to backfill existing parts.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                expression,
                type_,
                granularity=granularity,
                if_not_exists=if_not_exists,
                first=first,
                after_index=after_index,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseIndexOp(
            self.table_name,
            self.name,
            if_exists=True,
            schema=self.schema,
            clickhouse_settings=self.clickhouse_settings,
        )


@Operations.implementation_for(_AddClickHouseIndexOp)
def _add_clickhouse_index(operations: Operations, operation: _AddClickHouseIndexOp) -> Any:
    index = ClickHouseIndex(
        name=operation.name,
        expression=operation.expression,
        type_=operation.type_,
        granularity=operation.granularity,
        if_not_exists=operation.if_not_exists,
        first=operation.first,
        after_index=operation.after_index,
    )
    ft = full_table(operation.table_name, operation.schema)
    sql = f"ALTER TABLE {ft} {_render_add_index(index)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("add_clickhouse_indexes")
class _AddClickHouseIndexesOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        indexes: Sequence[ClickHouseIndex],
        *,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.indexes = tuple(indexes)
        if not self.indexes:
            raise ValueError("add_clickhouse_indexes requires at least one index")
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def add_clickhouse_indexes(
        cls,
        operations: Operations,
        table_name: str,
        indexes: Sequence[ClickHouseIndex],
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ONE comma-joined ALTER TABLE ... ADD INDEX, ADD INDEX ... statement.

        This is the fix for Code 517 CANNOT_ASSIGN_ALTER races on replicated deployments.
        Combining the subcommands is safe on both plain and Replicated databases because
        every subcommand is a homogeneous pure-metadata alter. Metadata-only: no mutation
        is scheduled, so no sync setting applies. Call materialize_clickhouse_index per
        index to backfill existing parts.
        """
        return operations.invoke(
            cls(
                table_name,
                indexes,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseIndexesOp(
            self.table_name,
            [index.name for index in self.indexes],
            if_exists=True,
            schema=self.schema,
            clickhouse_settings=self.clickhouse_settings,
        )


@Operations.implementation_for(_AddClickHouseIndexesOp)
def _add_clickhouse_indexes(operations: Operations, operation: _AddClickHouseIndexesOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    subcommands = ", ".join(_render_add_index(index) for index in operation.indexes)
    sql = f"ALTER TABLE {ft} {subcommands}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_index")
class _DropClickHouseIndexOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.name = name
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_index(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... DROP INDEX.

        Schedules a mutation governed by alter_sync (recommend 0, 1, or 2), not mutations_sync.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseIndexOp)
def _drop_clickhouse_index(operations: Operations, operation: _DropClickHouseIndexOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    sql = f"ALTER TABLE {ft} DROP INDEX {exists}{quote_identifier(operation.name)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_indexes")
class _DropClickHouseIndexesOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        names: Sequence[str],
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.names = tuple(names)
        if not self.names:
            raise ValueError("drop_clickhouse_indexes requires at least one index name")
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_indexes(
        cls,
        operations: Operations,
        table_name: str,
        names: Sequence[str],
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ONE comma-joined ALTER TABLE ... DROP INDEX, DROP INDEX ... statement."""
        return operations.invoke(
            cls(
                table_name,
                names,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseIndexesOp)
def _drop_clickhouse_indexes(operations: Operations, operation: _DropClickHouseIndexesOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    subcommands = ", ".join(f"DROP INDEX {exists}{quote_identifier(name)}" for name in operation.names)
    sql = f"ALTER TABLE {ft} {subcommands}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("materialize_clickhouse_index")
class _MaterializeClickHouseIndexOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        *,
        if_exists: bool = False,
        partition: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.name = name
        self.if_exists = if_exists
        self.partition = partition
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def materialize_clickhouse_index(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        if_exists: bool = False,
        partition: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... MATERIALIZE INDEX to backfill existing parts.

        partition is raw SQL passthrough. Schedules a mutation governed by mutations_sync
        (recommend 0, 1, or 2). Kept as a separate statement by design: Replicated databases
        reject ADD INDEX and MATERIALIZE INDEX combined in one statement.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                if_exists=if_exists,
                partition=partition,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_MaterializeClickHouseIndexOp)
def _materialize_clickhouse_index(operations: Operations, operation: _MaterializeClickHouseIndexOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    partition = f" IN PARTITION {operation.partition}" if operation.partition is not None else ""
    sql = (
        f"ALTER TABLE {ft} MATERIALIZE INDEX {exists}{quote_identifier(operation.name)}"
        f"{partition}{_settings_suffix(operation.clickhouse_settings)}"
    )
    return _exec_sql(operations, sql)


@Operations.register_operation("add_clickhouse_projection")
class _AddClickHouseProjectionOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        select: str,
        *,
        if_not_exists: bool = False,
        first: bool = False,
        after_projection: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        _validate_position(first, after_projection, self.__class__.__name__)
        self.table_name = table_name
        self.name = name
        self.select = select
        self.if_not_exists = if_not_exists
        self.first = first
        self.after_projection = after_projection
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def add_clickhouse_projection(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        select: str,
        if_not_exists: bool = False,
        first: bool = False,
        after_projection: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... ADD PROJECTION.

        select is the raw SQL body placed inside the parens. Metadata-only: no mutation is
        scheduled and existing parts are not backfilled, so no sync setting applies. Call
        materialize_clickhouse_projection to backfill existing parts.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                select,
                if_not_exists=if_not_exists,
                first=first,
                after_projection=after_projection,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseProjectionOp(
            self.table_name,
            self.name,
            if_exists=True,
            schema=self.schema,
            clickhouse_settings=self.clickhouse_settings,
        )


@Operations.implementation_for(_AddClickHouseProjectionOp)
def _add_clickhouse_projection(operations: Operations, operation: _AddClickHouseProjectionOp) -> Any:
    projection = ClickHouseProjection(
        name=operation.name,
        select=operation.select,
        if_not_exists=operation.if_not_exists,
        first=operation.first,
        after_projection=operation.after_projection,
    )
    ft = full_table(operation.table_name, operation.schema)
    sql = f"ALTER TABLE {ft} {_render_add_projection(projection)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("add_clickhouse_projections")
class _AddClickHouseProjectionsOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        projections: Sequence[ClickHouseProjection],
        *,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.projections = tuple(projections)
        if not self.projections:
            raise ValueError("add_clickhouse_projections requires at least one projection")
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def add_clickhouse_projections(
        cls,
        operations: Operations,
        table_name: str,
        projections: Sequence[ClickHouseProjection],
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ONE comma-joined ALTER TABLE ... ADD PROJECTION, ADD PROJECTION ... statement.

        This is the fix for Code 517 CANNOT_ASSIGN_ALTER races on replicated deployments.
        Combining the subcommands is safe on both plain and Replicated databases because
        every subcommand is a homogeneous pure-metadata alter. Metadata-only: no mutation
        is scheduled, so no sync setting applies. Call materialize_clickhouse_projection per
        projection to backfill existing parts.
        """
        return operations.invoke(
            cls(
                table_name,
                projections,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseProjectionsOp(
            self.table_name,
            [projection.name for projection in self.projections],
            if_exists=True,
            schema=self.schema,
            clickhouse_settings=self.clickhouse_settings,
        )


@Operations.implementation_for(_AddClickHouseProjectionsOp)
def _add_clickhouse_projections(operations: Operations, operation: _AddClickHouseProjectionsOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    subcommands = ", ".join(_render_add_projection(projection) for projection in operation.projections)
    sql = f"ALTER TABLE {ft} {subcommands}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_projection")
class _DropClickHouseProjectionOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.name = name
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_projection(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... DROP PROJECTION.

        Schedules a mutation governed by alter_sync (recommend 0, 1, or 2), not mutations_sync.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseProjectionOp)
def _drop_clickhouse_projection(operations: Operations, operation: _DropClickHouseProjectionOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    sql = f"ALTER TABLE {ft} DROP PROJECTION {exists}{quote_identifier(operation.name)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_projections")
class _DropClickHouseProjectionsOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        names: Sequence[str],
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.names = tuple(names)
        if not self.names:
            raise ValueError("drop_clickhouse_projections requires at least one projection name")
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_projections(
        cls,
        operations: Operations,
        table_name: str,
        names: Sequence[str],
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ONE comma-joined ALTER TABLE ... DROP PROJECTION, DROP PROJECTION ... statement."""
        return operations.invoke(
            cls(
                table_name,
                names,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseProjectionsOp)
def _drop_clickhouse_projections(operations: Operations, operation: _DropClickHouseProjectionsOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    subcommands = ", ".join(f"DROP PROJECTION {exists}{quote_identifier(name)}" for name in operation.names)
    sql = f"ALTER TABLE {ft} {subcommands}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("materialize_clickhouse_projection")
class _MaterializeClickHouseProjectionOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        name: str,
        *,
        if_exists: bool = False,
        partition: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.table_name = table_name
        self.name = name
        self.if_exists = if_exists
        self.partition = partition
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def materialize_clickhouse_projection(
        cls,
        operations: Operations,
        table_name: str,
        name: str,
        if_exists: bool = False,
        partition: str | None = None,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... MATERIALIZE PROJECTION to backfill existing parts.

        partition is raw SQL passthrough. Schedules a mutation governed by mutations_sync
        (recommend 0, 1, or 2). Kept as a separate statement by design: Replicated databases
        reject ADD PROJECTION and MATERIALIZE PROJECTION combined in one statement.
        """
        return operations.invoke(
            cls(
                table_name,
                name,
                if_exists=if_exists,
                partition=partition,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_MaterializeClickHouseProjectionOp)
def _materialize_clickhouse_projection(operations: Operations, operation: _MaterializeClickHouseProjectionOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    exists = "IF EXISTS " if operation.if_exists else ""
    partition = f" IN PARTITION {operation.partition}" if operation.partition is not None else ""
    sql = (
        f"ALTER TABLE {ft} MATERIALIZE PROJECTION {exists}{quote_identifier(operation.name)}"
        f"{partition}{_settings_suffix(operation.clickhouse_settings)}"
    )
    return _exec_sql(operations, sql)


@Operations.register_operation("modify_clickhouse_table_settings")
class _ModifyClickHouseTableSettingsOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        settings: Mapping[str, Any],
        *,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        if not settings:
            raise ValueError("modify_clickhouse_table_settings requires at least one setting")
        self.table_name = table_name
        self.settings = settings
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def modify_clickhouse_table_settings(
        cls,
        operations: Operations,
        table_name: str,
        settings: Mapping[str, Any],
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... MODIFY SETTING.

        settings are the table-level settings to change. clickhouse_settings is the separate
        query-level SETTINGS clause. Metadata-only, no mutation to wait on. Raises ValueError
        if settings is empty.
        """
        return operations.invoke(
            cls(
                table_name,
                settings,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_ModifyClickHouseTableSettingsOp)
def _modify_clickhouse_table_settings(operations: Operations, operation: _ModifyClickHouseTableSettingsOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    sql = f"ALTER TABLE {ft} MODIFY SETTING {render_settings(operation.settings)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("reset_clickhouse_table_settings")
class _ResetClickHouseTableSettingsOp(MigrateOperation):
    def __init__(
        self,
        table_name: str,
        names: Sequence[str],
        *,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        if not names:
            raise ValueError("reset_clickhouse_table_settings requires at least one setting name")
        self.table_name = table_name
        self.names = tuple(names)
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def reset_clickhouse_table_settings(
        cls,
        operations: Operations,
        table_name: str,
        names: Sequence[str],
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit ALTER TABLE ... RESET SETTING.

        names are bare setting names to reset to their defaults. Metadata-only, no mutation
        to wait on. Raises ValueError if names is empty.
        """
        return operations.invoke(
            cls(
                table_name,
                names,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_ResetClickHouseTableSettingsOp)
def _reset_clickhouse_table_settings(operations: Operations, operation: _ResetClickHouseTableSettingsOp) -> Any:
    ft = full_table(operation.table_name, operation.schema)
    names = ", ".join(operation.names)
    sql = f"ALTER TABLE {ft} RESET SETTING {names}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("create_clickhouse_materialized_view")
class _CreateClickHouseMaterializedViewOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        to_table: str,
        select: str,
        *,
        if_not_exists: bool = False,
        schema: str | None = None,
        to_schema: str | None = None,
    ) -> None:
        self.name = name
        self.to_table = to_table
        self.select = select
        self.if_not_exists = if_not_exists
        self.schema = schema
        self.to_schema = to_schema

    @classmethod
    def create_clickhouse_materialized_view(
        cls,
        operations: Operations,
        name: str,
        to_table: str,
        select: str,
        if_not_exists: bool = False,
        schema: str | None = None,
        to_schema: str | None = None,
    ) -> Any:
        """Emit CREATE MATERIALIZED VIEW ... TO ... AS ...

        select is raw SQL passthrough. This helper intentionally supports only the
        TO-table form because ENGINE and POPULATE are not valid with TO. ClickHouse
        treats SETTINGS after AS SELECT as part of the stored SELECT, so this helper
        does not accept clickhouse_settings.
        """
        return operations.invoke(
            cls(
                name,
                to_table,
                select,
                if_not_exists=if_not_exists,
                schema=schema,
                to_schema=to_schema,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseMaterializedViewOp(
            self.name,
            if_exists=True,
            schema=self.schema,
        )


@Operations.implementation_for(_CreateClickHouseMaterializedViewOp)
def _create_clickhouse_materialized_view(operations: Operations, operation: _CreateClickHouseMaterializedViewOp) -> Any:
    exists = " IF NOT EXISTS" if operation.if_not_exists else ""
    sql = (
        f"CREATE MATERIALIZED VIEW{exists} {full_table(operation.name, operation.schema)} "
        f"TO {full_table(operation.to_table, operation.to_schema)} AS {operation.select}"
    )
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_materialized_view")
class _DropClickHouseMaterializedViewOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_materialized_view(
        cls,
        operations: Operations,
        name: str,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit DROP VIEW for a ClickHouse materialized view."""
        return operations.invoke(
            cls(
                name,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseMaterializedViewOp)
def _drop_clickhouse_materialized_view(operations: Operations, operation: _DropClickHouseMaterializedViewOp) -> Any:
    exists = " IF EXISTS" if operation.if_exists else ""
    sql = f"DROP VIEW{exists} {full_table(operation.name, operation.schema)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("create_clickhouse_dictionary")
class _CreateClickHouseDictionaryOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        columns: Sequence[Column],
        *,
        primary_key: str,
        source: str,
        layout: str,
        lifetime: str,
        if_not_exists: bool = False,
        schema: str | None = None,
        comment: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.columns = tuple(columns)
        if not self.columns:
            raise ValueError("create_clickhouse_dictionary requires at least one column")
        self.primary_key = primary_key
        self.source = source
        self.layout = layout
        self.lifetime = lifetime
        self.if_not_exists = if_not_exists
        self.schema = schema
        self.comment = comment
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def create_clickhouse_dictionary(
        cls,
        operations: Operations,
        name: str,
        columns: Sequence[Column],
        *,
        primary_key: str,
        source: str,
        layout: str,
        lifetime: str,
        if_not_exists: bool = False,
        schema: str | None = None,
        comment: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit CREATE DICTIONARY.

        source, layout, lifetime, and primary_key are raw ClickHouse SQL fragments.
        Columns are SQLAlchemy Column objects rendered through the ClickHouse DDL compiler.
        """
        return operations.invoke(
            cls(
                name,
                columns,
                primary_key=primary_key,
                source=source,
                layout=layout,
                lifetime=lifetime,
                if_not_exists=if_not_exists,
                schema=schema,
                comment=comment,
                clickhouse_settings=clickhouse_settings,
            )
        )

    def reverse(self) -> MigrateOperation:
        return _DropClickHouseDictionaryOp(
            self.name,
            if_exists=True,
            schema=self.schema,
            clickhouse_settings=self.clickhouse_settings,
        )


@Operations.implementation_for(_CreateClickHouseDictionaryOp)
def _create_clickhouse_dictionary(operations: Operations, operation: _CreateClickHouseDictionaryOp) -> Any:
    exists = " IF NOT EXISTS" if operation.if_not_exists else ""
    layout = operation.layout if "(" in operation.layout else f"{operation.layout}()"
    sql = (
        f"CREATE DICTIONARY{exists} {full_table(operation.name, operation.schema)} "
        f"({_render_column_list(operations, operation.columns)}) "
        f"PRIMARY KEY {operation.primary_key} SOURCE({operation.source}) "
        f"LAYOUT({layout}) LIFETIME({operation.lifetime})"
    )
    settings = _settings_suffix(operation.clickhouse_settings)
    if operation.comment is not None:
        sql += f" COMMENT {format_str(operation.comment)}"
    elif settings:
        # ClickHouse parses bare SETTINGS after LIFETIME as dictionary settings.
        # COMMENT ends the dictionary body so the trailing SETTINGS is query-level.
        sql += " COMMENT ''"
    sql += settings
    return _exec_sql(operations, sql)


@Operations.register_operation("drop_clickhouse_dictionary")
class _DropClickHouseDictionaryOp(MigrateOperation):
    def __init__(
        self,
        name: str,
        *,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.if_exists = if_exists
        self.schema = schema
        self.clickhouse_settings = clickhouse_settings

    @classmethod
    def drop_clickhouse_dictionary(
        cls,
        operations: Operations,
        name: str,
        if_exists: bool = False,
        schema: str | None = None,
        clickhouse_settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Emit DROP DICTIONARY."""
        return operations.invoke(
            cls(
                name,
                if_exists=if_exists,
                schema=schema,
                clickhouse_settings=clickhouse_settings,
            )
        )


@Operations.implementation_for(_DropClickHouseDictionaryOp)
def _drop_clickhouse_dictionary(operations: Operations, operation: _DropClickHouseDictionaryOp) -> Any:
    exists = " IF EXISTS" if operation.if_exists else ""
    sql = f"DROP DICTIONARY{exists} {full_table(operation.name, operation.schema)}{_settings_suffix(operation.clickhouse_settings)}"
    return _exec_sql(operations, sql)


@Operations.register_operation("reload_clickhouse_dictionary")
class _ReloadClickHouseDictionaryOp(MigrateOperation):
    def __init__(self, name: str, *, schema: str | None = None) -> None:
        self.name = name
        self.schema = schema

    @classmethod
    def reload_clickhouse_dictionary(
        cls,
        operations: Operations,
        name: str,
        schema: str | None = None,
    ) -> Any:
        """Emit SYSTEM RELOAD DICTIONARY.

        Synchronous and blocking. Runs only on the node that receives it, not through the
        replicated DDL queue.
        """
        return operations.invoke(cls(name, schema=schema))


@Operations.implementation_for(_ReloadClickHouseDictionaryOp)
def _reload_clickhouse_dictionary(operations: Operations, operation: _ReloadClickHouseDictionaryOp) -> Any:
    sql = f"SYSTEM RELOAD DICTIONARY {full_table(operation.name, operation.schema)}"
    return _exec_sql(operations, sql)
