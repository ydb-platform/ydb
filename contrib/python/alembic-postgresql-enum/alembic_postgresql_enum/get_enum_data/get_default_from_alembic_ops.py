from typing import Optional, Dict, Tuple

from alembic.operations.ops import UpgradeOps, ModifyTableOps, AddColumnOp, AlterColumnOp, CreateTableOp
from sqlalchemy import Column

SchemaName = str
TableName = str
ColumnName = str
ColumnLocation = Tuple[SchemaName, TableName, ColumnName]


def _get_default_from_add_column_op(op: AddColumnOp, default_schema: str) -> Tuple[ColumnLocation, Optional[str]]:
    if op.column.server_default is None:
        raise AttributeError("No new server_default")
    return (
        (op.schema or default_schema, op.table_name, op.column.name),
        op.column.server_default.arg.text,  # type: ignore[attr-defined]
    )


def _get_default_from_alter_column_op(op: AlterColumnOp, default_schema: str) -> Tuple[ColumnLocation, Optional[str]]:
    if op.modify_server_default is False:
        raise AttributeError("No new server_default")
    return (op.schema or default_schema, op.table_name, op.column_name), op.modify_server_default


def _get_default_from_column(column: Column, default_schema: str) -> Tuple[ColumnLocation, Optional[str]]:
    if column.server_default is None:
        raise AttributeError("No new server_default")
    return (
        (column.table.schema or default_schema, column.table.name, column.name),
        column.server_default.arg.text,  # type: ignore[attr-defined]
    )


def get_just_added_defaults(
    upgrade_ops: Optional[UpgradeOps], default_schema: str
) -> Dict[ColumnLocation, Optional[str]]:
    """Get all server defaults that will be added in current migration"""
    if upgrade_ops is None:
        return {}

    new_server_defaults = {}

    for operations_group in upgrade_ops.ops:
        if isinstance(operations_group, ModifyTableOps):
            for operation in operations_group.ops:
                if isinstance(operation, AddColumnOp):
                    try:
                        column_location, column_new_default = _get_default_from_add_column_op(operation, default_schema)
                        new_server_defaults[column_location] = column_new_default
                    except AttributeError:
                        pass

                elif isinstance(operation, AlterColumnOp):
                    try:
                        column_location, column_new_default = _get_default_from_alter_column_op(
                            operation, default_schema
                        )
                        new_server_defaults[column_location] = column_new_default
                    except AttributeError:
                        pass

        elif isinstance(operations_group, CreateTableOp):
            for column in operations_group.columns:
                if isinstance(column, Column):
                    try:
                        column_location, column_new_default = _get_default_from_column(column, default_schema)
                        new_server_defaults[column_location] = column_new_default
                    except AttributeError:
                        pass

    return new_server_defaults
