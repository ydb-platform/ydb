from typing import TYPE_CHECKING, Callable

from alembic_postgresql_enum.get_enum_data.types import EnumNamesToValues
from alembic_postgresql_enum.sql_commands.enum_type import get_all_enums

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def _extract_enum_name(enum_name: str, schema: str) -> str:
    # Remove schema
    schema_prefix = f"{schema}."
    quoted_schema_prefix = f'"{schema}".'
    if enum_name.startswith(schema_prefix):
        enum_name = enum_name[len(schema_prefix) :]
    elif enum_name.startswith(quoted_schema_prefix):
        enum_name = enum_name[len(quoted_schema_prefix) :]

    # Remove quotes
    if enum_name.startswith('"') and enum_name.startswith('"'):
        enum_name = enum_name[1:-1]

    return enum_name


def get_defined_enums(
    connection: "Connection", schema: str, include_name: Callable[[str], bool] = lambda _: True
) -> EnumNamesToValues:
    """
    Return a dict mapping PostgreSQL defined enumeration types to the set of their
    defined values.
    :param conn:
        SQLAlchemy connection instance.
    :param str schema:
        Schema name (e.g. "public").
    :param Callable[[str], bool] include_name:
        Callable that returns True if the enum name should be included
    :returns DeclaredEnumValues:
        enum_definitions={
            "my_enum": tuple(["a", "b", "c"]),
        }
    """

    return {
        enum_name: tuple(values)
        for enum_name, values in (
            (_extract_enum_name(name, schema), values) for name, values in get_all_enums(connection, schema)
        )
        if include_name(enum_name)
    }
