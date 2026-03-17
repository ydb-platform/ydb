from typing import Final

from django_test_migrations.db.checks.statement_timeout import (
    check_statement_timeout_setting,
)

#: We use this value as a unique identifier of databases related check.
CHECK_NAME: Final = 'django_test_migrations.checks.database_configuration'
CHECKS: Final = (check_statement_timeout_setting,)
