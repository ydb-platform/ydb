import datetime
from typing import Final

from django.core.checks import CheckMessage
from django.core.checks import Warning as DjangoWarning
from django.db import connections

from django_test_migrations.db.backends import exceptions, registry
from django_test_migrations.db.backends.base.configuration import (
    BaseDatabaseConfiguration,
)
from django_test_migrations.logic.datetime import timedelta_to_milliseconds
from django_test_migrations.types import AnyConnection

#: We use this value as a unique identifier of databases related check.
CHECK_NAME: Final = 'django_test_migrations.checks.database_configuration'
STATEMENT_TIMEOUT_MINUTES_UPPER_LIMIT: Final = 30


def check_statement_timeout_setting(
    *args: object,
    **kwargs: object,
) -> list[CheckMessage]:
    """Check if statements' timeout settings is properly configured."""
    messages: list[CheckMessage] = []
    for connection in connections.all():
        _check_statement_timeout_setting(connection, messages)
    return messages


def _check_statement_timeout_setting(
    connection: AnyConnection,
    messages: list[CheckMessage],
) -> None:
    try:
        database_configuration = registry.get_database_configuration(
            connection,
        )
    except exceptions.DatabaseConfigurationNotFound:
        return

    try:
        setting_value = int(
            database_configuration.get_setting_value(
                database_configuration.statement_timeout,
            )
        )
    except exceptions.DatabaseConfigurationSettingNotFound:
        return

    _ensure_statement_timeout_is_set(
        database_configuration,
        setting_value,
        messages,
    )
    _ensure_statement_timeout_not_too_high(
        database_configuration,
        setting_value,
        messages,
    )


def _ensure_statement_timeout_is_set(
    database_configuration: BaseDatabaseConfiguration,
    setting_value: int,
    messages: list[CheckMessage],
) -> None:
    if not setting_value:
        connection = database_configuration.connection
        messages.append(
            DjangoWarning(
                (
                    f'{connection.alias}: statement timeout'
                    ' "{database_configuration.statement_timeout}" '
                    'setting is not set.'
                ),
                hint=(
                    f'Set "{database_configuration.statement_timeout}" database'
                    ' setting to some reasonable value.'
                ),
                id=f'{CHECK_NAME}.W001',
            ),
        )


def _ensure_statement_timeout_not_too_high(
    database_configuration: BaseDatabaseConfiguration,
    setting_value: int,
    messages: list[CheckMessage],
) -> None:
    upper_limit = timedelta_to_milliseconds(
        datetime.timedelta(minutes=STATEMENT_TIMEOUT_MINUTES_UPPER_LIMIT),
    )
    if setting_value > upper_limit:
        messages.append(
            DjangoWarning(
                (
                    '{0}: statement timeout "{1}" setting value - "{2} ms" '
                    + 'might be too high.'
                ).format(
                    database_configuration.connection.alias,
                    database_configuration.statement_timeout,
                    setting_value,
                ),
                hint=(
                    'Set "{0}" database setting to some '
                    + 'reasonable value, but remember it should not be '
                    + 'too high.'
                ).format(database_configuration.statement_timeout),
                id=f'{CHECK_NAME}.W002',
            ),
        )
