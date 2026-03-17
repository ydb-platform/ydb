import threading

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import DEFAULT_DB_ALIAS
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property

from clickhouse_backend import driver as Database  # NOQA

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor


class DatabaseWrapper(BaseDatabaseWrapper):
    _connection_sharing_lock = threading.Lock()
    _clickhouse_connections = {}

    vendor = "clickhouse"
    display_name = "ClickHouse"
    # This dictionary maps Field objects to their associated ClickHouse column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    data_types = {
        # Django fields.
        "SmallAutoField": "Int64",
        "AutoField": "Int64",
        "BigAutoField": "Int64",
        "IPAddressField": "IPv4",
        "GenericIPAddressField": "IPv6",
        "JSONField": "Object('json')",
        "BinaryField": "String",
        "CharField": "FixedString(%(max_length)s)",
        "DateField": "Date32",
        "DateTimeField": "DateTime64(6, 'UTC')" if settings.USE_TZ else "DateTime64(6)",
        "DecimalField": "Decimal(%(max_digits)s, %(decimal_places)s)",
        "FileField": "String",
        "FilePathField": "String",
        "FloatField": "Float64",
        "SmallIntegerField": "Int16",
        "IntegerField": "Int32",
        "BigIntegerField": "Int64",
        "PositiveBigIntegerField": "UInt64",
        "PositiveIntegerField": "UInt32",
        "PositiveSmallIntegerField": "UInt16",
        "SlugField": "String",
        "TextField": "String",
        "UUIDField": "UUID",
        "BoolField": "Bool",
        "BooleanField": "Bool",
        # Clickhouse fields.
        "Int8Field": "Int8",
        "Int16Field": "Int16",
        "Int32Field": "Int32",
        "Int64Field": "Int64",
        "Int128Field": "Int128",
        "Int256Field": "Int256",
        "UInt8Field": "UInt8",
        "UInt16Field": "UInt16",
        "UInt32Field": "UInt32",
        "UInt64Field": "UInt64",
        "UInt128Field": "UInt128",
        "UInt256Field": "UInt256",
        "Float32Field": "Float32",
        "Float64Field": "Float64",
        "StringField": "String",
        "FixedStringField": "FixedString(%(max_bytes)s)",
        "ClickhouseDateField": "Date",
        "Date32Field": "Date32",
        "ClickhouseDateTimeField": "DateTime('UTC')" if settings.USE_TZ else "DateTime",
        "DateTime64Field": "DateTime64(%(precision)s, 'UTC')"
        if settings.USE_TZ
        else "DateTime64(%(precision)s)",
        "EnumField": "Enum",
        "Enum8Field": "Enum8",
        "Enum16Field": "Enum16",
        "ArrayField": "Array",  # Not used by ArrayField.db_type
        "TupleField": "Tuple",  # Not used by TupleField.db_type
        "MapField": "Map",  # Not used by MapField.db_type
        "IPv4Field": "IPv4",
        "IPv6Field": "IPv6",
    }
    low_cardinality_data_types = {
        "Int8Field",
        "Int16Field",
        "Int32Field",
        "Int64Field",
        "Int128Field",
        "Int256Field",
        "UInt8Field",
        "UInt16Field",
        "UInt32Field",
        "UInt64Field",
        "UInt128Field",
        "UInt256Field",
        "Float32Field",
        "Float64Field",
        "BooleanField",
        "StringField",
        "FixedStringField",
        "UUIDField",
        "ClickhouseDateField",
        "Date32Field",
        "ClickhouseDateTimeField",
        "IPv4Field",
        "IPv6Field",
        "GenericIPAddressField",
    }
    operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "ILIKE %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "ILIKE %s",
        "iendswith": "ILIKE %s",
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as "%" is used as a wildcard for
    # the LIKE operator.
    pattern_esc = r"replaceRegexpAll({}, '\\\\|%%|_', '\\\\\\0')"
    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "ILIKE '%%' || {} || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "ILIKE {} || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "ILIKE '%%' || {}",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        super().__init__(settings_dict, alias)
        # Use fake_transaction control whether using fake transaction.
        # Fake transaction is used in test, prevent other database such as postgresql
        # from flush at the end of each testcase. Only use this feature when you are
        # aware of the effect in TransactionTestCase.
        self._fake_transaction = False
        self.migration_cluster = self.settings_dict["OPTIONS"].pop(
            "migration_cluster", None
        )
        self.distributed_migrations = self.settings_dict["OPTIONS"].pop(
            "distributed_migrations", None
        )
        # https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#streaming-results
        self.max_block_size = self.settings_dict["OPTIONS"].pop("max_block_size", 65409)
        if not self.settings_dict["NAME"]:
            self.settings_dict["NAME"] = "default"

    @property
    def fake_transaction(self):
        return self._fake_transaction

    @fake_transaction.setter
    def fake_transaction(self, value):
        self._fake_transaction = value
        self.features.fake_transaction = self._fake_transaction

    def get_connection_params(self):
        settings_dict = self.settings_dict
        if len(settings_dict["NAME"] or "") > self.ops.max_name_length():
            raise ImproperlyConfigured(
                "The database name '%s' (%d characters) is longer than "
                "Clickhouse's limit of %d characters. Supply a shorter NAME "
                "in settings.DATABASES."
                % (
                    settings_dict["NAME"],
                    len(settings_dict["NAME"]),
                    self.ops.max_name_length(),
                )
            )

        conn_params = {
            "host": settings_dict["HOST"] or "localhost",
            **settings_dict.get("OPTIONS", {}),
        }
        if settings_dict["NAME"]:
            conn_params["database"] = settings_dict["NAME"]
        if settings_dict["USER"]:
            conn_params["user"] = settings_dict["USER"]
        if settings_dict["PASSWORD"]:
            conn_params["password"] = settings_dict["PASSWORD"]
        if settings_dict["PORT"]:
            conn_params["port"] = settings_dict["PORT"]
        return conn_params

    @async_unsafe
    def get_new_connection(self, conn_params):
        # Fix https://github.com/jayvynl/django-clickhouse-backend/issues/53
        with self._connection_sharing_lock:
            if self.alias in self._clickhouse_connections:
                params, conn = self._clickhouse_connections[self.alias]
                if conn_params == params:
                    return conn

            conn = Database.connect(**conn_params)
            self._clickhouse_connections[self.alias] = (conn_params, conn)

        return conn

    def init_connection_state(self):
        pass

    @async_unsafe
    def create_cursor(self, name=None):
        return self.connection.cursor()

    @async_unsafe
    def chunked_cursor(self):
        cursor = self._cursor()
        cursor.cursor.set_stream_results(True, self.max_block_size)
        return cursor

    def set_autocommit(
        self, autocommit, force_begin_transaction_with_broken_autocommit=False
    ):
        self.autocommit = autocommit

    def commit(self):
        pass

    def _savepoint(self, sid):
        pass

    def _savepoint_rollback(self, sid):
        pass

    def _savepoint_commit(self, sid):
        pass

    def _close(self):
        """Close database connection.

        This is a noop, because inner connection is shared between threads.
        """
        pass

    def is_usable(self):
        try:
            # Use a clickhouse_driver cursor directly, bypassing Django's utilities.
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except Database.Error:
            return False
        else:
            return True

    @cached_property
    def ch_version(self):
        with self.temporary_connection() as cursor:
            cursor.execute("SELECT version()")
            row = cursor.fetchone()
        return row[0]

    def get_database_version(self):
        """
        Return a tuple of the database's version.
        E.g. for ch_version "22.9.3.18", return (22, 9, 3, 18).
        """
        return tuple(map(int, self.ch_version.split(".")))
