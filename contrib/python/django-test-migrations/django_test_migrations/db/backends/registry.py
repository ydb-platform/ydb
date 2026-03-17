from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from django_test_migrations.db.backends.exceptions import (
    DatabaseConfigurationNotFound,
)
from django_test_migrations.types import AnyConnection

if TYPE_CHECKING:
    from django_test_migrations.db.backends.base.configuration import (
        BaseDatabaseConfiguration,
    )

_DatabaseConfigurationMapping = MutableMapping[
    str,
    type['BaseDatabaseConfiguration'],
]
database_configuration_registry: _DatabaseConfigurationMapping = {}


def get_database_configuration(
    connection: AnyConnection,
) -> 'BaseDatabaseConfiguration':
    """Return proper ``BaseDatabaseConfiguration`` subclass instance.

    Raises:
        DatabaseConfigurationNotFound
            when vendor extracted from ``connection`` doesn't support
            interaction with database configuration/settings
    """
    vendor = getattr(connection, 'vendor', '')
    try:
        database_configuration_class = database_configuration_registry[vendor]
    except KeyError as exc:
        raise DatabaseConfigurationNotFound(vendor) from exc
    return database_configuration_class(connection)
