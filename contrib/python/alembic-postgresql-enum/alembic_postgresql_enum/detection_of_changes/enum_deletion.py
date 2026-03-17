import logging

from alembic.operations.ops import UpgradeOps

from alembic_postgresql_enum.get_enum_data import EnumNamesToValues
from alembic_postgresql_enum.operations.drop_enum import DropEnumOp

log = logging.getLogger(f"alembic.{__name__}")


def drop_unused_enums(
    defined_enums: EnumNamesToValues,
    declared_enums: EnumNamesToValues,
    schema: str,
    upgrade_ops: UpgradeOps,
):
    """
    Drop enums that are in Postgres schema but not declared in SqlAlchemy schema
    """
    for name, new_values in defined_enums.items():
        if name not in declared_enums:
            log.info("Detected unused enum %r with values %r", name, new_values)
            upgrade_ops.ops.append(DropEnumOp(name=name, schema=schema, enum_values=new_values))
