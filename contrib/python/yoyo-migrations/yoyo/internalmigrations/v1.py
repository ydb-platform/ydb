"""
Version 1 schema.

Compatible with yoyo-migrations < 6.0
"""


def upgrade(backend):
    create_migration_table(backend)


def create_migration_table(backend):
    backend.execute(
        "CREATE TABLE {0.migration_table_quoted} ("
        "id VARCHAR(191) NOT NULL PRIMARY KEY,"
        "ctime TIMESTAMP)".format(backend)
    )
