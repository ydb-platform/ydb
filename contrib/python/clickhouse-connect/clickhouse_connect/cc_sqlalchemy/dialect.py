import sqlalchemy.schema as sa_schema
from sqlalchemy import text
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoResultFound, NoSuchTableError

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy import dialect_name, ischema_names
from clickhouse_connect.cc_sqlalchemy.inspector import ChInspector, get_table_metadata
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.compiler import ChStatementCompiler
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ChDDLCompiler
from clickhouse_connect.cc_sqlalchemy.sql.preparer import ChIdentifierPreparer
from clickhouse_connect.driver.binding import format_str, quote_identifier


class ClickHouseDialect(DefaultDialect):
    """
    See :py:class:`sqlalchemy.engine.interfaces`
    """

    name = dialect_name
    driver = "connect"

    default_schema_name = "default"
    supports_native_decimal = True
    supports_native_boolean = True
    supports_statement_cache = False
    supports_comments = True
    inline_comments = True
    returns_unicode_strings = True
    postfetch_lastrowid = False
    ddl_compiler = ChDDLCompiler
    statement_compiler = ChStatementCompiler
    preparer = ChIdentifierPreparer
    description_encoding = None
    max_identifier_length = 127
    ischema_names = ischema_names
    inspector = ChInspector
    construct_arguments = [
        (
            sa_schema.Table,
            {
                "engine": None,
                "table_type": None,
                "dictionary_source": None,
                "dictionary_layout": None,
                "dictionary_lifetime": None,
                "dictionary_primary_key": None,
            },
        ),
        (
            sa_schema.Column,
            {
                "materialized": None,
                "alias": None,
                "codec": None,
                "ttl": None,
                "after": None,
                "settings": None,
            },
        ),
    ]

    # SQA 1 compatibility

    @classmethod
    def dbapi(cls):
        return dbapi

    # SQA 2 compatibility

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def _get_default_schema_name(self, connection):
        return connection.execute(text("SELECT currentDatabase()")).scalar()

    def get_schema_names(self, connection, **_):
        return [row.name for row in connection.execute(text("SHOW DATABASES"))]

    @staticmethod
    def has_database(connection, db_name):
        return (connection.execute(text(f"SELECT name FROM system.databases WHERE name = {format_str(db_name)}"))).rowcount > 0

    def get_table_names(self, connection, schema=None, **kw):
        cmd = "SHOW TABLES"
        if schema:
            cmd += " FROM " + quote_identifier(schema)
        return [row.name for row in connection.execute(text(cmd))]

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        raise NoSuchTableError(f"{schema}.{view_name}" if schema else view_name)

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            table_metadata = get_table_metadata(connection, table_name, schema)
        except NoResultFound:
            raise NoSuchTableError(f"{schema}.{table_name}" if schema else table_name) from None
        return {"text": table_metadata.comment or None}

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, **_kw):
        result = connection.execute(text(f"EXISTS TABLE {full_table(table_name, schema)}"))
        row = result.fetchone()
        return row[0] == 1

    def has_sequence(self, connection, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return "AUTOCOMMIT"
