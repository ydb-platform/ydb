from functools import cached_property
from typing import cast

from typing_extensions import final

from django_test_migrations.db.backends.base.configuration import (
    BaseDatabaseConfiguration,
)
from django_test_migrations.types import DatabaseSettingValue


@final
class MySQLDatabaseConfiguration(BaseDatabaseConfiguration):
    """Interact with MySQL database configuration."""

    vendor = 'mysql'

    def get_setting_value(self, name: str) -> DatabaseSettingValue:
        """Retrieve value of MySQL database's setting with ``name``."""
        with self.connection.cursor() as cursor:
            quoted = self.connection.ops.quote_name(name)
            cursor.execute(f'SELECT @@{quoted};')
            setting_value = cursor.fetchone()
            if not setting_value:
                return super().get_setting_value(name)
            return cast(DatabaseSettingValue, setting_value[0])

    @cached_property
    def version(self) -> str:
        """Get MySQL DB server version."""
        return str(self.get_setting_value('VERSION'))

    @property
    def statement_timeout(self) -> str:
        """Get `STATEMENT TIMEOUT` setting name based on DB server version."""
        if 'mariadb' in self.version.lower():
            return 'MAX_STATEMENT_TIME'
        return 'MAX_EXECUTION_TIME'
