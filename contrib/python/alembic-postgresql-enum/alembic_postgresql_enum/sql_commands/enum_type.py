from typing import TYPE_CHECKING, List, Tuple

import sqlalchemy

from alembic_postgresql_enum.get_enum_data import TableReference, ColumnType

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def cast_old_array_enum_type_to_new(
    connection: "Connection",
    table_reference: TableReference,
    enum_type_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
):
    cast_clause = f"{table_reference.escaped_column_name}::text[]"

    for old_value, new_value in enum_values_to_rename:
        cast_clause = f"""array_replace({cast_clause}, '{old_value}', '{new_value}')"""

    connection.execute(
        sqlalchemy.text(
            f"""ALTER TABLE {table_reference.table_name_with_schema} 
            ALTER COLUMN {table_reference.escaped_column_name} TYPE {enum_type_name}[]
            USING {cast_clause}::{enum_type_name}[]
            """
        )
    )


def cast_old_enum_type_to_new(
    connection: "Connection",
    table_reference: TableReference,
    enum_type_name: str,
    enum_values_to_rename: List[Tuple[str, str]],
):
    if table_reference.column_type == ColumnType.ARRAY:
        cast_old_array_enum_type_to_new(connection, table_reference, enum_type_name, enum_values_to_rename)
        return

    if enum_values_to_rename:
        connection.execute(
            sqlalchemy.text(
                f"""ALTER TABLE {table_reference.table_name_with_schema} 
                ALTER COLUMN {table_reference.escaped_column_name} TYPE {enum_type_name} 
                USING CASE 
                {' '.join(
                f"WHEN {table_reference.escaped_column_name}::text = '{old_value}' THEN '{new_value}'::{enum_type_name}"
                for old_value, new_value in enum_values_to_rename)}

                ELSE {table_reference.escaped_column_name}::text::{enum_type_name}
                END
                """
            )
        )
    else:
        connection.execute(
            sqlalchemy.text(
                f"""ALTER TABLE {table_reference.table_name_with_schema} 
                ALTER COLUMN {table_reference.escaped_column_name} TYPE {enum_type_name} 
                USING {table_reference.escaped_column_name}::text::{enum_type_name}
                """
            )
        )


def drop_type(connection: "Connection", enum_type_name: str):
    connection.execute(sqlalchemy.text(f"""DROP TYPE {enum_type_name}"""))


def rename_type(connection: "Connection", enum_type_name: str, new_type_name: str):
    connection.execute(sqlalchemy.text(f"""ALTER TYPE {enum_type_name} RENAME TO {new_type_name}"""))


def create_type(connection: "Connection", enum_type_name: str, enum_values: List[str]):
    connection.execute(
        sqlalchemy.text(f"""CREATE TYPE {enum_type_name} AS ENUM({', '.join(f"'{value}'" for value in enum_values)})""")
    )


def get_all_enums(connection: "Connection", schema: str):
    sql = """
        SELECT
            pg_catalog.format_type(t.oid, NULL),
            ARRAY(SELECT enumlabel
                  FROM pg_catalog.pg_enum
                  WHERE enumtypid = t.oid
                  ORDER BY enumsortorder)
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE
            t.typtype = 'e'
            AND n.nspname = :schema
    """
    return connection.execute(sqlalchemy.text(sql), dict(schema=schema))
