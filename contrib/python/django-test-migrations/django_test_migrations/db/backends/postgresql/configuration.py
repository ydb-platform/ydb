from typing import cast

from typing_extensions import final

from django_test_migrations.db.backends.base.configuration import (
    BaseDatabaseConfiguration,
)
from django_test_migrations.types import DatabaseSettingValue


@final
class PostgreSQLDatabaseConfiguration(BaseDatabaseConfiguration):
    """Interact with PostgreSQL database configuration."""

    vendor = 'postgresql'
    statement_timeout = 'statement_timeout'

    def get_setting_value(self, name: str) -> DatabaseSettingValue:
        """Retrieve value of PostgreSQL database's setting with ``name``."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                (
                    'SELECT setting FROM pg_settings '  # noqa: S608
                    + 'WHERE name = %s;'
                ),
                (name,),
            )
            setting_value = cursor.fetchone()
            if not setting_value:
                return super().get_setting_value(name)
            return cast(DatabaseSettingValue, setting_value[0])
