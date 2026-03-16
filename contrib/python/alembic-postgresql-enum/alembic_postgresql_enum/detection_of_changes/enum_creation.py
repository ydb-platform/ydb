import logging

from alembic.operations.ops import UpgradeOps

from alembic_postgresql_enum.get_enum_data import EnumNamesToValues
from alembic_postgresql_enum.operations.create_enum import CreateEnumOp

log = logging.getLogger(f"alembic.{__name__}")


def create_new_enums(
    defined_enums: EnumNamesToValues,
    declared_enums: EnumNamesToValues,
    schema: str,
    upgrade_ops: UpgradeOps,
):
    """
    Create enums that are not in Postgres schema
    """
    for name, new_values in declared_enums.items():
        if name not in defined_enums:
            log.info("Detected added enum %r with values %r", name, new_values)
            upgrade_ops.ops.insert(0, CreateEnumOp(name=name, schema=schema, enum_values=new_values))
