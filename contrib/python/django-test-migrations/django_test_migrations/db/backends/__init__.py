# register all ``BaseDatabaseConfiguration`` subclasses
from django_test_migrations.db.backends.mysql.configuration import (
    MySQLDatabaseConfiguration as MySQLDatabaseConfiguration,
)
from django_test_migrations.db.backends.postgresql.configuration import (
    PostgreSQLDatabaseConfiguration as PostgreSQLDatabaseConfiguration,
)
