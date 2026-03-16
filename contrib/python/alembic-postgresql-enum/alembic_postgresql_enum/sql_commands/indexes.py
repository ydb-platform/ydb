from typing import TYPE_CHECKING, List, Tuple, Set, NamedTuple

import sqlalchemy


if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


class TableIndex(NamedTuple):
    """Represents an index that needs to be recreated."""

    name: str
    definition: str

    def __repr__(self):
        return f"TableIndex(name={self.name!r}, definition={self.definition!r})"


def get_dependent_indexes(
    connection: "Connection",
    enum_schema: str,
    enum_name: str,
) -> List[TableIndex]:
    """
    Get all indexes that depend on the enum type.
    Returns a list of TableIndex objects.
    """
    enum_oid_result = connection.execute(
        sqlalchemy.text(
            """
            SELECT t.oid
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = :schema AND t.typname = :name
            """
        ),
        {"schema": enum_schema, "name": enum_name},
    ).fetchone()

    if not enum_oid_result:
        return []

    enum_oid = enum_oid_result.oid

    # Find all indexes that depend on this enum type
    # This includes indexes on columns of this enum type and partial indexes referencing it
    result = connection.execute(
        sqlalchemy.text(
            """
            SELECT DISTINCT
                idx_ns.nspname || '.' || idx_class.relname as index_name,
                pg_get_indexdef(idx_class.oid) as index_def
            FROM pg_depend dep
            -- Join to get the index details
            JOIN pg_class idx_class ON dep.objid = idx_class.oid
            JOIN pg_namespace idx_ns ON idx_class.relnamespace = idx_ns.oid
            -- Join to get the index info
            JOIN pg_index idx ON idx.indexrelid = idx_class.oid
            WHERE 
                -- The dependency is on our enum type
                dep.refobjid = :enum_oid
                AND dep.refclassid = 'pg_type'::regclass
                -- The dependent object is an index
                AND dep.classid = 'pg_class'::regclass
                AND idx_class.relkind = 'i'
                -- Only include partial indexes (those with WHERE clauses)
                AND idx.indpred IS NOT NULL
            
            UNION
            
            -- Also check for indexes where the predicate references the enum
            -- This catches cases where the enum is used in the WHERE clause
            -- but might not have a direct dependency (e.g., cast expressions)
            SELECT DISTINCT
                n.nspname || '.' || c.relname as index_name,
                pg_get_indexdef(i.indexrelid) as index_def
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indpred IS NOT NULL
                AND pg_get_expr(i.indpred, i.indrelid)::text LIKE '%' || :enum_name || '%'
                -- Additional check to ensure it's actually our enum
                AND EXISTS (
                    SELECT 1 FROM pg_depend d
                    WHERE d.objid = i.indexrelid
                    AND d.refobjid = :enum_oid
                    AND d.refclassid = 'pg_type'::regclass
                )
            """
        ),
        {"enum_oid": enum_oid, "enum_name": enum_name},
    )

    indexes = []
    for row in result:
        indexes.append(TableIndex(name=row.index_name, definition=row.index_def))

    return indexes


def drop_indexes(connection: "Connection", indexes: List[TableIndex]):
    """
    Drop the specified indexes.
    """
    for index in indexes:
        index_name = index.name
        # Extract schema and index name if qualified
        if "." in index_name:
            schema_name, idx_name = index_name.rsplit(".", 1)
            schema_name = schema_name.strip('"')
            idx_name = idx_name.strip('"')
            connection.execute(sqlalchemy.text(f'DROP INDEX IF EXISTS "{schema_name}"."{idx_name}"'))
        else:
            idx_name = index_name.strip('"')
            connection.execute(sqlalchemy.text(f'DROP INDEX IF EXISTS "{idx_name}"'))


def recreate_indexes(connection: "Connection", indexes: List[TableIndex]):
    """
    Recreate the indexes using their stored definitions.
    """
    for index in indexes:
        connection.execute(sqlalchemy.text(index.definition))


def transform_indexes_for_renamed_values(
    indexes: List[TableIndex], enum_name: str, enum_values_to_rename: List[Tuple[str, str]], enum_schema: str
) -> List[TableIndex]:
    """
    Transform all indexes to use renamed enum values.
    Returns a new list of TableIndex objects with transformed definitions.
    """
    if not enum_values_to_rename:
        return indexes

    transformed_indexes = []
    for index in indexes:
        transformed_def = transform_index_definition_for_renamed_values(
            index.definition, enum_name, enum_values_to_rename, enum_schema
        )
        transformed_indexes.append(TableIndex(name=index.name, definition=transformed_def))

    return transformed_indexes


def transform_index_definition_for_renamed_values(
    index_definition: str, enum_name: str, enum_values_to_rename: List[Tuple[str, str]], enum_schema: str
) -> str:
    """
    Transform an index definition to use renamed enum values.

    For each (old_value, new_value) pair, replaces occurrences of:
    - 'old_value'::enum_name with 'new_value'::enum_name (unqualified)
    - 'old_value'::schema.enum_name with 'new_value'::schema.enum_name (schema-qualified)
    """
    transformed_def = index_definition

    for old_value, new_value in enum_values_to_rename:
        old_pattern = f"'{old_value}'::{enum_name}"
        new_pattern = f"'{new_value}'::{enum_name}"
        transformed_def = transformed_def.replace(old_pattern, new_pattern)

        # If schema provided, also replace any schema-qualified references
        if enum_schema:
            old_qualified = f"'{old_value}'::{enum_schema}.{enum_name}"
            new_qualified = f"'{new_value}'::{enum_schema}.{enum_name}"
            transformed_def = transformed_def.replace(old_qualified, new_qualified)

    return transformed_def
