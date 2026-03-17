import re
import typing as T
from contextlib import contextmanager

from clickhouse_driver import connection
from clickhouse_driver.dbapi import connection as dbapi_connection
from clickhouse_driver.dbapi import cursor, errors
from clickhouse_driver.result import IterQueryResult, ProgressQueryResult, QueryResult
from django.conf import settings

from .escape import escape_params
from .pool import ClickhousePool

name_regex = r'"(?:[^"]|\\.)+"'
value_regex = r"(')?(?(1)(?:[^']|\\.)+|\S+)(?(1)'|)"
name_value_regex = f"{name_regex} = {value_regex}"
update_pattern = re.compile(f"^ALTER TABLE ({name_regex}) UPDATE ")


def send_query(self, query, query_id=None, params=None):
    if not self.connected:
        self.connect()

    connection.write_varint(connection.ClientPacketTypes.QUERY, self.fout)

    connection.write_binary_str(query_id or "", self.fout)

    revision = self.server_info.used_revision
    if revision >= connection.defines.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
        client_info = connection.ClientInfo(
            self.client_name, self.context, client_revision=self.client_revision
        )
        client_info.query_kind = connection.ClientInfo.QueryKind.INITIAL_QUERY

        client_info.write(revision, self.fout)

    settings_as_strings = (
        revision
        >= connection.defines.DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
    )
    settings_flags = 0
    if self.settings_is_important:
        settings_flags |= connection.SettingsFlags.IMPORTANT
    connection.write_settings(
        self.context.settings, self.fout, settings_as_strings, settings_flags
    )

    if revision >= connection.defines.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET:
        connection.write_binary_str("", self.fout)

    connection.write_varint(connection.QueryProcessingStage.COMPLETE, self.fout)
    connection.write_varint(self.compression, self.fout)

    connection.write_binary_str(query, self.fout)

    if revision >= connection.defines.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS:
        if self.context.client_settings["server_side_params"]:
            # Always settings_as_strings = True
            escaped = escape_params(params or {}, self.context, for_server=True)
        else:
            escaped = {}
        connection.write_settings(
            escaped, self.fout, True, connection.SettingsFlags.CUSTOM
        )

    connection.logger.debug("Query: %s", query)

    self.fout.flush()


# Monkey patch to resolve https://github.com/jayvynl/django-clickhouse-backend/issues/14
connection.Connection.send_query = send_query


class Cursor(cursor.Cursor):
    # Whether to return data in columnar format. For backwards-compatibility,
    # let's default to None.
    columnar = None

    def close(self):
        """Push client back to connection pool"""
        if self.closed:
            return
        self._state = self._states.CURSOR_CLOSED
        self._connection.pool.push(self._client)

    @property
    def closed(self):
        return self._state == self._states.CURSOR_CLOSED

    @property
    def use_numpy(self):
        return self._client.client_settings["use_numpy"]

    @use_numpy.setter
    def use_numpy(self, value):
        self._client.client_settings["use_numpy"] = value
        if value:
            try:
                from clickhouse_driver.numpy.result import (
                    NumpyIterQueryResult,
                    NumpyProgressQueryResult,
                    NumpyQueryResult,
                )

                self._client.query_result_cls = NumpyQueryResult
                self._client.iter_query_result_cls = NumpyIterQueryResult
                self._client.progress_query_result_cls = NumpyProgressQueryResult
            except ImportError as e:
                raise RuntimeError("Extras for NumPy must be installed") from e
        else:
            self._client.query_result_cls = QueryResult
            self._client.iter_query_result_cls = IterQueryResult
            self._client.progress_query_result_cls = ProgressQueryResult

    @contextmanager
    def set_query_execution_args(
        self, columnar: T.Optional[bool] = None, use_numpy: T.Optional[bool] = None
    ):
        original_use_numpy = self.use_numpy
        if use_numpy is not None:
            self.use_numpy = use_numpy

        original_columnar = self.columnar
        if columnar is not None:
            self.columnar = columnar

        yield self

        self.use_numpy = original_use_numpy
        self.columnar = original_columnar

    def __del__(self):
        # If someone forgets calling close method,
        # then release connection when gc happens.
        if not self.closed:
            self.close()

    def _prepare(self):
        """Override clickhouse_driver.Cursor._prepare() to add columnar kwargs.

        See https://github.com/jayvynl/django-clickhouse-backend/issues/119
        """
        execute, execute_kwargs = super()._prepare()
        if self.columnar is not None:
            execute_kwargs["columnar"] = self.columnar
        return execute, execute_kwargs

    def execute(self, operation, parameters=None):
        """fix https://github.com/jayvynl/django-clickhouse-backend/issues/9"""
        if getattr(
            settings, "CLICKHOUSE_ENABLE_UPDATE_ROWCOUNT", True
        ) and update_pattern.match(operation):
            query = self._client.substitute_params(
                operation, parameters, self._client.connection.context
            )
            m = update_pattern.match(query)
            table = m.group(1)
            query_upper = query.upper()
            i = query_upper.rfind(" WHERE ")
            if i > 0:
                j = query_upper.rfind(" SETTINGS ", i + 7)
                if j > 0:
                    where = query[i + 7 : j]
                else:
                    where = query[i + 7 :]
                super().execute(f"select count(*) from {table} where {where}")
                (rowcount,) = self.fetchone()
                self._reset_state()
                self._rowcount = rowcount
        super().execute(operation, parameters)


class Connection(dbapi_connection.Connection):
    """Connection class with support for connection pool."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("connections_min", 10)
        kwargs.setdefault("connections_max", 100)
        super().__init__(*args, **kwargs)
        self.pool = ClickhousePool(
            dsn=self.dsn,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            **self.connection_kwargs,
        )

    def _make_client(self):
        """
        :return: a new Client instance.
        """
        return self.pool.pull()

    def close(self):
        self.pool.cleanup()
        self.is_closed = True

    def cursor(self, cursor_factory=Cursor):
        """Use clickhouse_backend.connection.Cursor which support
        connection pool to create cursor."""
        if self.is_closed:
            raise errors.InterfaceError("connection already closed")

        client = self._make_client()
        if self._hosts is None:
            self._hosts = client.connection.hosts
        else:
            client.connection.hosts = self._hosts
        cursor_factory = cursor_factory or Cursor
        return cursor_factory(client, self)
