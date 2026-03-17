import re
from typing import TYPE_CHECKING, Union, List, Tuple

import sqlalchemy

from alembic_postgresql_enum.get_enum_data.types import TableReference

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def get_column_default(
    connection: "Connection", table_schema: str, table_name: str, column_name: str
) -> Union[str, None]:
    """Result example: "'active'::order_status" """
    default_value = connection.execute(
        sqlalchemy.text(
            f"""
        SELECT column_default
        FROM information_schema.columns
        WHERE 
            table_schema = '{table_schema}' AND 
            table_name = '{table_name}' AND 
            column_name = '{column_name}'
    """
        )
    ).scalar()
    return default_value


def drop_default(
    connection: "Connection",
    table_reference: TableReference,
):
    connection.execute(
        sqlalchemy.text(
            f"""ALTER TABLE {table_reference.table_name_with_schema}
             ALTER COLUMN {table_reference.escaped_column_name} DROP DEFAULT"""
        )
    )


def set_default(
    connection: "Connection",
    table_reference: TableReference,
    default_value: str,
):
    connection.execute(
        sqlalchemy.text(
            f"""ALTER TABLE {table_reference.table_name_with_schema}
            ALTER COLUMN {table_reference.escaped_column_name} SET DEFAULT {default_value}"""
        )
    )


def rename_default_if_required(  # todo smells like teen shit
    schema: str,
    default_value: str,
    enum_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
) -> str:
    if schema:
        new_enum = f"{schema}.{enum_name}"
    else:
        new_enum = enum_name

    if default_value.startswith("ARRAY["):
        column_default_value = _replace_strings_in_quotes(default_value, enum_values_to_rename)
        column_default_value = re.sub(r"::[.\w]+", f"::{new_enum}", column_default_value)
        return column_default_value

    if default_value.endswith("[]"):
        # remove old type postfix
        column_default_value = default_value[: default_value.find("::")]

        column_default_value = _replace_strings_in_quotes(column_default_value, enum_values_to_rename)

        return f"{column_default_value}::{new_enum}[]"

    # remove old type postfix
    column_default_value = default_value[: default_value.find("::")]

    column_default_value = _replace_strings_in_quotes(column_default_value, enum_values_to_rename)

    return f"{column_default_value}::{new_enum}"


def _replace_strings_in_quotes(
    old_default: str,
    enum_values_to_rename: List[Tuple[str, str]],
) -> str:
    for old_value, new_value in enum_values_to_rename:
        old_default = old_default.replace(f"'{old_value}'", f"'{new_value}'")
        old_default = old_default.replace(f'"{old_value}"', f'"{new_value}"')
    return old_default
