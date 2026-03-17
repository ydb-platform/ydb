from collections import defaultdict
from typing import Tuple, Any, Set, Union, List, TYPE_CHECKING, cast, Optional, Callable

import sqlalchemy
from alembic.operations.ops import UpgradeOps
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql

from alembic_postgresql_enum.get_enum_data.get_default_from_alembic_ops import get_just_added_defaults
from alembic_postgresql_enum.sql_commands.column_default import get_column_default

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

from alembic_postgresql_enum.get_enum_data import (
    DeclaredEnumValues,
    TableReference,
    ColumnType,
)


def get_enum_values(enum_type: sqlalchemy.Enum) -> "Tuple[str, ...]":
    # For specific case when types.TypeDecorator is used
    if isinstance(enum_type, sqlalchemy.types.TypeDecorator):
        dialect = postgresql.dialect

        def value_processor(value):
            return enum_type.process_bind_param(enum_type.impl.result_processor(dialect, enum_type)(value), dialect)

    else:

        def value_processor(enum_value):
            return enum_value

    return tuple(value_processor(value) for value in enum_type.enums)


def column_type_is_enum(column_type: Any) -> bool:
    if isinstance(column_type, sqlalchemy.Enum):
        return column_type.native_enum

    # For specific case when types.TypeDecorator is used
    if isinstance(getattr(column_type, "impl", None), sqlalchemy.Enum):
        return True

    return False


def get_declared_enums(
    metadata: Union[MetaData, List[MetaData]],
    schema: str,
    default_schema: str,
    connection: "Connection",
    upgrade_ops: Optional[UpgradeOps] = None,
    include_name: Callable[[str], bool] = lambda _: True,
) -> DeclaredEnumValues:
    """
    Return a dict mapping SQLAlchemy declared enumeration types to the set of their values
    with columns where enums are used.
    :param metadata:
        SqlAlchemy schema
    :param str schema:
        Schema name (e.g. "public").
    :param default_schema:
        Default schema name, likely will be "public"
    :param connection:
        Database connection
    :param upgrade_ops:
        Upgrade operations in current migration
    :returns DeclaredEnumValues:
        enum_values: {
            "my_enum": tuple(["a", "b", "c"]),
        },
        enum_table_references: {
            "my_enum": {
                EnumToTable(table_name="my_table", column_name="my_column")
            }
        }
    """
    enum_name_to_values = dict()
    enum_name_to_table_references: defaultdict[str, Set[TableReference]] = defaultdict(set)

    just_added_defaults = get_just_added_defaults(upgrade_ops, default_schema)

    if isinstance(metadata, list):
        metadata_list = metadata
    else:
        metadata_list = [metadata]

    for metadata in metadata_list:
        for table in metadata.tables.values():
            for column in table.columns:
                column_type = column.type
                column_type_wrapper = ColumnType.COMMON

                # if column is array of enums
                if isinstance(column_type, sqlalchemy.ARRAY):
                    column_type = column_type.item_type
                    column_type_wrapper = ColumnType.ARRAY

                if not column_type_is_enum(column_type):
                    continue

                if not include_name(column_type.name):
                    continue

                column_type_schema = column_type.schema or default_schema  # type: ignore[attr-defined]
                if column_type_schema != schema:
                    continue

                if column_type.name not in enum_name_to_values:  # type: ignore[attr-defined]
                    enum_name_to_values[column_type.name] = get_enum_values(cast(sqlalchemy.Enum, column_type))  # type: ignore[attr-defined]

                table_schema = table.schema or default_schema
                column_default = get_column_default(connection, table_schema, table.name, column.name)
                if (table_schema, table.name, column.name) in just_added_defaults:
                    column_default = just_added_defaults[table_schema, table.name, column.name]
                enum_name_to_table_references[column_type.name].add(  # type: ignore[attr-defined]
                    TableReference(
                        table_schema=table_schema,
                        table_name=table.name,
                        column_name=column.name,
                        column_type=column_type_wrapper,
                        existing_server_default=column_default,
                    )
                )

    return DeclaredEnumValues(
        enum_values=enum_name_to_values,
        enum_table_references={
            enum_name: frozenset(table_references)
            for enum_name, table_references in enum_name_to_table_references.items()
        },
    )
