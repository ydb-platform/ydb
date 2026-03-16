from typing import Final

from django.core.management.color import Style, no_style
from django.db import connections

DJANGO_MIGRATIONS_TABLE_NAME: Final = 'django_migrations'


def drop_models_tables(
    database_name: str,
    style: Style | None = None,
) -> None:
    """Drop all installed Django's models tables."""
    style = style or no_style()
    connection = connections[database_name]
    tables = connection.introspection.django_table_names(
        only_existing=True,
        include_views=False,
    )
    sql_drop_tables = [
        connection.SchemaEditorClass.sql_delete_table
        % {
            'table': style.SQL_FIELD(connection.ops.quote_name(table)),
        }
        for table in tables
    ]
    if sql_drop_tables:
        if connection.vendor == 'mysql':
            sql_drop_tables = [
                'SET FOREIGN_KEY_CHECKS = 0;',
                *sql_drop_tables,
                'SET FOREIGN_KEY_CHECKS = 1;',
            ]
        connection.ops.execute_sql_flush(sql_drop_tables)


def flush_django_migrations_table(
    database_name: str,
    style: Style | None = None,
) -> None:
    """Flush `django_migrations` table.

    `django_migrations` is not "regular" Django model, so its not returned
    by ``ConnectionRouter.get_migratable_models`` which is used e.g. to
    implement sequences reset.

    """
    connection = connections[database_name]
    connection.ops.execute_sql_flush(
        connection.ops.sql_flush(
            style or no_style(),
            [DJANGO_MIGRATIONS_TABLE_NAME],
            allow_cascade=False,
            reset_sequences=True,
        ),
    )
