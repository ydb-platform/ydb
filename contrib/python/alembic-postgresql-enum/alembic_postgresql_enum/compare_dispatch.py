import logging
from typing import Iterable, Union

import alembic
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps, CreateTableOp

from alembic_postgresql_enum.add_create_type_false import add_create_type_false
from alembic_postgresql_enum.add_postgres_using_to_text import (
    add_postgres_using_to_text,
)
from alembic_postgresql_enum.detection_of_changes import (
    sync_changed_enums,
    create_new_enums,
    drop_unused_enums,
)
from alembic_postgresql_enum.get_enum_data import get_defined_enums, get_declared_enums
from alembic_postgresql_enum.configuration import get_configuration

log = logging.getLogger(f"alembic.{__name__}")


def compare_enums(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema_names: Iterable[Union[str, None]],
):
    """
    Walk the declared SQLAlchemy schema for every referenced Enum, walk the PG
    schema for every defined Enum, then generate SyncEnumValuesOp migrations
    for each defined enum that has changed new entries when compared to its
    declared version.
    """
    assert (
        autogen_context.dialect is not None
        and autogen_context.dialect.default_schema_name is not None
        and autogen_context.connection is not None
        and autogen_context.metadata is not None
    )

    configuration = get_configuration()

    if autogen_context.dialect.name != "postgresql" and not configuration.force_dialect_support:
        log.warning(
            f"This library only supports postgresql, but you are using {autogen_context.dialect.name}, skipping"
        )
        return

    add_create_type_false(upgrade_ops)
    add_postgres_using_to_text(upgrade_ops)

    schema_names = list(schema_names)

    # Issue #40
    # Add schema if it is gonna be created inside the migration
    for operations_group in upgrade_ops.ops:
        if isinstance(operations_group, CreateTableOp) and operations_group.schema not in schema_names:
            schema_names.append(operations_group.schema)

    for schema in schema_names:
        default_schema = autogen_context.dialect.default_schema_name
        if schema is None:
            schema = default_schema

        definitions = get_defined_enums(autogen_context.connection, schema, configuration.include_name)

        declarations = get_declared_enums(
            autogen_context.metadata,
            schema,
            default_schema,
            autogen_context.connection,
            upgrade_ops,
            configuration.include_name,
        )

        create_new_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        if configuration.drop_unused_enums:
            drop_unused_enums(definitions, declarations.enum_values, schema, upgrade_ops)

        if configuration.detect_enum_values_changes:
            sync_changed_enums(
                definitions,
                declarations.enum_values,
                declarations.enum_table_references,
                schema,
                upgrade_ops,
                connection=autogen_context.connection,
            )


if tuple((int(s) for s in alembic.__version__.split("."))) < (1, 18, 0):
    alembic.autogenerate.comparators.dispatch_for("schema")(compare_enums)
else:
    from alembic.util import DispatchPriority

    alembic.autogenerate.comparators.dispatch_for("schema", priority=DispatchPriority.LAST)(compare_enums)
