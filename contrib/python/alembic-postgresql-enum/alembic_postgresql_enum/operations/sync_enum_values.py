import logging
from typing import List, Tuple, Any, Iterable, TYPE_CHECKING, Optional

import alembic.autogenerate
import alembic.operations.base
import alembic.operations.ops
from alembic.autogenerate.api import AutogenContext
from sqlalchemy.exc import DataError

from alembic_postgresql_enum.configuration import get_configuration
from alembic_postgresql_enum.get_enum_data.types import Unspecified
from alembic_postgresql_enum.sql_commands.column_default import (
    drop_default,
    get_column_default,
    rename_default_if_required,
    set_default,
)
from alembic_postgresql_enum.sql_commands.comparison_operators import (
    create_comparison_operators,
    drop_comparison_operators,
)
from alembic_postgresql_enum.sql_commands.indexes import (
    TableIndex,
    drop_indexes,
    recreate_indexes,
    transform_indexes_for_renamed_values,
)
from alembic_postgresql_enum.sql_commands.enum_type import (
    cast_old_enum_type_to_new,
    create_type,
    drop_type,
    rename_type,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

from alembic_postgresql_enum.connection import get_connection
from alembic_postgresql_enum.get_enum_data import ColumnType, TableReference

log = logging.getLogger(f"alembic.{__name__}")


@alembic.operations.base.Operations.register_operation("sync_enum_values")
class SyncEnumValuesOp(alembic.operations.ops.MigrateOperation):
    operation_name = "change_enum_variants"

    def __init__(
        self,
        schema: str,
        name: str,
        old_values: List[str],
        new_values: List[str],
        affected_columns: List[TableReference],
        affected_indexes: List[TableIndex] = None,
    ):
        self.schema = schema
        self.name = name
        self.old_values = old_values
        self.new_values = new_values
        self.affected_columns = affected_columns
        self.indexes_to_recreate = affected_indexes or []

    def reverse(self):
        """
        See MigrateOperation.reverse().
        """
        return SyncEnumValuesOp(
            self.schema,
            self.name,
            old_values=self.new_values,
            new_values=self.old_values,
            affected_columns=self.affected_columns,
            affected_indexes=self.indexes_to_recreate,
        )

    @classmethod
    def _set_enum_values(
        cls,
        connection: "Connection",
        enum_schema: str,
        enum_name: str,
        new_values: List[str],
        affected_columns: List[TableReference],
        enum_values_to_rename: List[Tuple[str, str]],
        indexes_to_recreate: List[TableIndex],
    ):
        enum_type_name = f'"{enum_schema}"."{enum_name}"'
        temporary_enum_name = f'"{enum_name}_old"'
        temporary_enum_type_name = f'"{enum_schema}".{temporary_enum_name}'

        if indexes_to_recreate and enum_values_to_rename:
            indexes_to_recreate = transform_indexes_for_renamed_values(
                indexes_to_recreate, enum_name, enum_values_to_rename, enum_schema
            )

        rename_type(connection, enum_type_name, temporary_enum_name)
        create_type(connection, enum_type_name, new_values)

        create_comparison_operators(connection, enum_type_name, temporary_enum_type_name, enum_values_to_rename)

        drop_indexes(connection, indexes_to_recreate)

        for table_reference in affected_columns:
            column_default = table_reference.existing_server_default

            if column_default is not None:
                drop_default(connection, table_reference)

            try:
                cast_old_enum_type_to_new(connection, table_reference, enum_type_name, enum_values_to_rename)
            except DataError as error:
                raise ValueError(
                    """New enum values can not be set due to some row containing reference to old enum value.
                        Please consider using enum_values_to_rename parameter or "
                    f"updating/deleting these row before calling sync_enum_values."""
                ) from error

            if column_default is not None:
                column_default = rename_default_if_required(
                    enum_schema, column_default, enum_name, enum_values_to_rename
                )

                set_default(connection, table_reference, column_default)

        drop_comparison_operators(connection, enum_type_name, temporary_enum_type_name)
        drop_type(connection, temporary_enum_type_name)

        recreate_indexes(connection, indexes_to_recreate)

    @classmethod
    def sync_enum_values(
        cls,
        operations,
        enum_schema: str,
        enum_name: str,
        new_values: List[str],
        affected_columns: List[Tuple[str, str]],
        enum_values_to_rename: Iterable[Tuple[str, str]] = tuple(),
        indexes_to_recreate: Optional[List[TableIndex]] = None,
    ):
        """
        Replace enum values with `new_values`
        :param operations:
            ...
        :param str enum_schema:
            Schema name.
        :param enum_name:
            Enumeration type name.
        :param list new_values:
            List of enumeration values that should exist after this migration
            executes.
        :param list affected_columns:
            List of columns that references this enum.
            First value is table_name,
            second value is column_name
        :param enum_values_to_rename:
            Iterable of tuples containing old_name and new_name
            enum_values_to_rename=[
                ('tree', 'three') # to fix typo
            ]
            If there was server default with old_name it will be renamed accordingly
        :param indexes_to_recreate:
            List of TableIndex objects representing indexes that need to be recreated.
            These are indexes that depend on the enum type and will be dropped and recreated
            during the migration. This parameter should be provided at migration generation time
            to support offline mode.
        """

        config = get_configuration()

        if operations.migration_context.dialect.name != "postgresql" and not config.force_dialect_support:
            log.warning(
                f"This library only supports postgresql, but you are using {operations.migration_context.dialect.name}, skipping"
            )
            return

        enum_values_to_rename = list(enum_values_to_rename)

        with get_connection(operations) as connection:
            table_references = []
            for affected_column in affected_columns:
                if isinstance(affected_column, tuple):  # This is considered old style
                    table_name = affected_column[0]
                    column_name = affected_column[1]
                    if len(affected_column) > 2:
                        column_type = affected_column[2]
                    else:
                        column_type = ColumnType.COMMON
                    column_default = get_column_default(connection, enum_schema, table_name, column_name)
                    table_references.append(
                        TableReference(
                            table_name,
                            column_name,
                            table_schema=enum_schema,
                            column_type=column_type,
                            existing_server_default=column_default,
                        )
                    )

                elif isinstance(affected_column, TableReference):
                    if affected_column.table_schema is Unspecified:
                        affected_column = TableReference(
                            table_name=affected_column.table_name,
                            column_name=affected_column.column_name,
                            table_schema=enum_schema,  # For backwards compatibility
                            column_type=affected_column.column_type,
                            existing_server_default=affected_column.existing_server_default,
                        )
                    table_references.append(affected_column)
                else:
                    raise ValueError("Affected columns must contain tuples or TableReferences")

            cls._set_enum_values(
                connection,
                enum_schema,
                enum_name,
                new_values,
                table_references,
                enum_values_to_rename,
                indexes_to_recreate or [],
            )

    def to_diff_tuple(self) -> Tuple[Any, ...]:
        return (
            self.operation_name,
            self.old_values,
            self.new_values,
            self.affected_columns,
        )

    @property
    def is_column_type_import_needed(self) -> bool:
        return any((affected_column.is_column_type_import_needed for affected_column in self.affected_columns))


@alembic.autogenerate.render.renderers.dispatch_for(SyncEnumValuesOp)
def render_sync_enum_value_op(autogen_context: AutogenContext, op: SyncEnumValuesOp):
    config = get_configuration()
    if op.is_column_type_import_needed:
        autogen_context.imports.add("from alembic_postgresql_enum import ColumnType")
    autogen_context.imports.add("from alembic_postgresql_enum import TableReference")
    alembic_module_prefix = autogen_context.opts.get("alembic_module_prefix", "op.")

    if op.indexes_to_recreate:
        autogen_context.imports.add("from alembic_postgresql_enum.sql_commands.indexes import TableIndex")

    lines = [
        f"{alembic_module_prefix}sync_enum_values({'  # type: ignore[attr-defined]' if config.add_type_ignore else ''}",
        f"    enum_schema={op.schema!r},",
        f"    enum_name={op.name!r},",
        f"    new_values={op.new_values!r},",
        f"    affected_columns={op.affected_columns!r},",
        f"    enum_values_to_rename=[],",
    ]

    if op.indexes_to_recreate:
        lines.append(f"    indexes_to_recreate=[")
        for index in op.indexes_to_recreate:
            lines.append(f"        TableIndex(")
            lines.append(f"            name={index.name!r},")
            lines.append(f"            definition={index.definition!r},")
            lines.append(f"        ),")
        lines.append(f"    ],")

    lines.append(f")")

    return "\n".join(lines)
