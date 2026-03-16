from testsuite import utils

from . import connection, pool

CREATE_DATABASE_TEMPLATE = """
CREATE DATABASE "{}" WITH TEMPLATE = template0
ENCODING='UTF8' LC_COLLATE='C' LC_CTYPE='C'
"""

DATABASE_EXISTS_TEMPLATE = 'SELECT 1 FROM pg_database WHERE datname=%s'

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS applied_schemas (
    db_name TEXT PRIMARY KEY,
    schema_hash TEXT
);
"""

UPDATE_DB_HASH_TEMPLATE = """
INSERT INTO applied_schemas (db_name, schema_hash)
VALUES (%(dbname)s, %(hash)s)
ON CONFLICT (db_name) DO UPDATE SET
    schema_hash = %(hash)s
WHERE applied_schemas.db_name = %(dbname)s
"""
SELECT_DB_HASH_TEMPLATE = 'SELECT db_name, schema_hash FROM applied_schemas'
TESTSUITE_DB_NAME = 'testsuite'


class AppliedSchemaHashes:
    def __init__(
        self,
        pool: pool.AutocommitConnectionPool,
        base_conninfo: connection.PgConnectionInfo,
    ):
        self._pool = pool
        self._conninfo = base_conninfo.replace(dbname=TESTSUITE_DB_NAME)

        self._create_db()
        self._create_schema_table()

    def get_hash(self, dbname: str) -> str | None:
        """Get hash of schema applied to a database"""
        return self._hash_by_dbname.get(dbname, None)

    def set_hash(self, dbname: str, schema_hash: str):
        """Store in testsuite database and remember locally a hash of schema
        applied to a database
        """
        self._hash_by_dbname[dbname] = schema_hash

        with self._pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    UPDATE_DB_HASH_TEMPLATE,
                    {'dbname': dbname, 'hash': schema_hash},
                )

    @utils.cached_property
    def _hash_by_dbname(self) -> dict[str, str]:
        with self._pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(SELECT_DB_HASH_TEMPLATE)
                return {entry[0]: entry[1] for entry in cursor}

    def _create_schema_table(self) -> None:
        with self._pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(CREATE_TABLE_SQL)

    def _create_db(self) -> None:
        with self._pool.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(DATABASE_EXISTS_TEMPLATE, (TESTSUITE_DB_NAME,))
                db_exists = any(cursor)
                if db_exists:
                    return
                cursor.execute(
                    CREATE_DATABASE_TEMPLATE.format(TESTSUITE_DB_NAME)
                )
