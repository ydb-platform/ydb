"""
Alembic extension to generate ALTER TYPE ... ADD VALUE statements to update
SQLAlchemy enums.

"""

import logging

from typing import TYPE_CHECKING

from alembic.operations.ops import UpgradeOps

from alembic_postgresql_enum.get_enum_data import (
    EnumNamesToValues,
    EnumNamesToTableReferences,
)
from alembic_postgresql_enum.operations.sync_enum_values import SyncEnumValuesOp
from alembic_postgresql_enum.configuration import get_configuration
from alembic_postgresql_enum.sql_commands.indexes import get_dependent_indexes

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

log = logging.getLogger(f"alembic.{__name__}")


def sync_changed_enums(
    defined_enums: EnumNamesToValues,
    declared_enums: EnumNamesToValues,
    table_references: EnumNamesToTableReferences,
    schema: str,
    upgrade_ops: UpgradeOps,
    connection: "Connection",
):
    configuration = get_configuration()

    for enum_name, new_values in declared_enums.items():
        if enum_name not in defined_enums:
            # That is work for create_new_enums function
            continue

        old_values = defined_enums[enum_name]

        are_values_equal = (
            new_values == old_values
            if not configuration.ignore_enum_values_order
            else sorted(new_values) == sorted(old_values)
        )
        if are_values_equal:
            # Enum definition and declaration are in sync
            continue

        log.info(
            "Detected changed enum values in %r\nWas: %r\nBecome: %r",
            enum_name,
            list(old_values),
            list(new_values),
        )
        affected_columns = table_references[enum_name]

        affected_indexes = get_dependent_indexes(connection, schema, enum_name)

        op = SyncEnumValuesOp(
            schema,
            enum_name,
            list(old_values),
            list(new_values),
            sorted(  # Sort references alphabetically for consistency of generated text
                affected_columns,
                key=lambda reference: (reference.table_schema, reference.table_name, reference.column_name),
            ),
            sorted(affected_indexes, key=lambda index: index.name),
        )
        upgrade_ops.ops.append(op)
