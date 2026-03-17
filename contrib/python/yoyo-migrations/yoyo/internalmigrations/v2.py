"""
Version 2 schema.

Compatible with yoyo-migrations >=  6.0
"""
from datetime import datetime
from yoyo.migrations import get_migration_hash


def upgrade(backend):
    qi = backend.quote_identifier
    create_log_table(backend)
    create_version_table(backend)
    cursor = backend.execute(
        f"SELECT {qi('id')}, {qi('ctime')} FROM {backend.migration_table_quoted}"
    )
    migration_id = ""
    created_at = datetime(1970, 1, 1)
    for migration_id, created_at in iter(cursor.fetchone, None):  # type: ignore
        migration_hash = get_migration_hash(migration_id)
        log_data = dict(
            backend.get_log_data(),
            operation="apply",
            comment=(
                "this log entry created automatically by an internal schema upgrade"
            ),
            created_at_utc=created_at,
            migration_hash=migration_hash,
            migration_id=migration_id,
        )
        qi = backend.quote_identifier
        backend.execute(
            f"""
            INSERT INTO {backend.log_table_quoted} (
                {qi('id')},
                {qi('migration_hash')},
                {qi('migration_id')},
                {qi('operation')},
                {qi('created_at_utc')},
                {qi('username')},
                {qi('hostname')},
                {qi('comment')}
            ) VALUES (
                :id, :migration_hash, :migration_id, 'apply', :created_at_utc,
                :username, :hostname, :comment
            )
            """,
            log_data,
        )

    backend.execute("DROP TABLE {0.migration_table_quoted}".format(backend))
    create_migration_table(backend)
    backend.execute(
        f"""
        INSERT INTO {backend.migration_table_quoted}
        SELECT {qi('migration_hash')}, {qi('migration_id')}, {qi('created_at_utc')}
        FROM {backend.log_table_quoted}
        """
    )


def create_migration_table(backend):
    qi = backend.quote_identifier
    backend.execute(
        # migration_hash: sha256 hash of the migration id
        # migration_id: identifier of the migration file
        #               (path basename without extension)
        # applied_at_utc: time in UTC of when the id was applied
        f"""
        CREATE TABLE {backend.migration_table_quoted} (
            {qi('migration_hash')} VARCHAR(64),
            {qi('migration_id')} VARCHAR(255),
            {qi('applied_at_utc')} TIMESTAMP,
            PRIMARY KEY ({qi('migration_hash')})
        )
        """
    )


def create_log_table(backend):
    qi = backend.quote_identifier
    backend.execute(
        f"""
        CREATE TABLE {backend.log_table_quoted} (
            {qi('id')} VARCHAR(36),
            {qi('migration_hash')} VARCHAR(64),
            {qi('migration_id')} VARCHAR(255),
            {qi('operation')} VARCHAR(10),
            {qi('username')} VARCHAR(255),
            {qi('hostname')} VARCHAR(255),
            {qi('comment')} VARCHAR(255),
            {qi('created_at_utc')} TIMESTAMP,
            PRIMARY KEY ({qi('id')})
        )
        """
    )


def create_version_table(backend):
    qi = backend.quote_identifier
    backend.execute(
        f"""
        CREATE TABLE {backend.version_table_quoted} (
            {qi('version')} INT NOT NULL PRIMARY KEY,
            {qi('installed_at_utc')} TIMESTAMP
        )
        """
    )
