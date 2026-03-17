"""
Experimental
Work in progress, breaking changes are possible.
"""

import collections
import collections.abc
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

import sqlalchemy as sa
import ydb
from sqlalchemy import util
from sqlalchemy.engine import characteristics, reflection
from sqlalchemy.engine.default import DefaultExecutionContext, StrCompileDialect
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import functions

from sqlalchemy.sql.elements import ClauseList

import ydb_dbapi
from ydb_sqlalchemy.sqlalchemy.dbapi_adapter import AdaptedAsyncConnection
from ydb_sqlalchemy.sqlalchemy.dml import Upsert

from ydb_sqlalchemy.sqlalchemy.compiler import YqlCompiler, YqlDDLCompiler, YqlIdentifierPreparer, YqlTypeCompiler

from . import types


OLD_SA = sa.__version__ < "2."


class ParametrizedFunction(functions.Function):
    __visit_name__ = "parametrized_function"

    def __init__(self, name, params, *args, **kwargs):
        super(ParametrizedFunction, self).__init__(name, *args, **kwargs)
        self._func_name = name
        self._func_params = params
        self.params_expr = ClauseList(operator=functions.operators.comma_op, group_contents=True, *params).self_group()


def upsert(table):
    return Upsert(table)


COLUMN_TYPES = {
    ydb.PrimitiveType.Int8: sa.INTEGER,
    ydb.PrimitiveType.Int16: sa.INTEGER,
    ydb.PrimitiveType.Int32: sa.INTEGER,
    ydb.PrimitiveType.Int64: sa.INTEGER,
    ydb.PrimitiveType.Uint8: sa.INTEGER,
    ydb.PrimitiveType.Uint16: sa.INTEGER,
    ydb.PrimitiveType.Uint32: types.UInt32,
    ydb.PrimitiveType.Uint64: types.UInt64,
    ydb.PrimitiveType.Float: sa.FLOAT,
    ydb.PrimitiveType.Double: sa.FLOAT,
    ydb.PrimitiveType.String: sa.BINARY,
    ydb.PrimitiveType.Utf8: sa.TEXT,
    ydb.PrimitiveType.Json: sa.JSON,
    ydb.PrimitiveType.JsonDocument: sa.JSON,
    ydb.DecimalType: sa.DECIMAL,
    ydb.PrimitiveType.Yson: sa.TEXT,
    ydb.PrimitiveType.Date: sa.DATE,
    ydb.PrimitiveType.Date32: sa.DATE,
    ydb.PrimitiveType.Timestamp64: sa.TIMESTAMP,
    ydb.PrimitiveType.Datetime64: sa.DATETIME,
    ydb.PrimitiveType.Datetime: sa.DATETIME,
    ydb.PrimitiveType.Timestamp: sa.TIMESTAMP,
    ydb.PrimitiveType.Interval: sa.INTEGER,
    ydb.PrimitiveType.Bool: sa.BOOLEAN,
    ydb.PrimitiveType.DyNumber: sa.TEXT,
}


def _get_column_info(t):
    nullable = False
    if isinstance(t, ydb.OptionalType):
        nullable = True
        t = t.item

    if isinstance(t, ydb.DecimalType):
        return sa.DECIMAL(precision=t.precision, scale=t.scale), nullable

    return COLUMN_TYPES[t], nullable


class YdbRequestSettingsCharacteristic(characteristics.ConnectionCharacteristic):
    def reset_characteristic(self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection) -> None:
        dialect.reset_ydb_request_settings(dbapi_connection)

    def set_characteristic(
        self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection, value: ydb.BaseRequestSettings
    ) -> None:
        dialect.set_ydb_request_settings(dbapi_connection, value)

    def get_characteristic(
        self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection
    ) -> ydb.BaseRequestSettings:
        return dialect.get_ydb_request_settings(dbapi_connection)


class YdbRetrySettingsCharacteristic(characteristics.ConnectionCharacteristic):
    def reset_characteristic(self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection) -> None:
        dialect.reset_ydb_retry_settings(dbapi_connection)

    def set_characteristic(
        self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection, value: ydb.RetrySettings
    ) -> None:
        dialect.set_ydb_retry_settings(dbapi_connection, value)

    def get_characteristic(self, dialect: "YqlDialect", dbapi_connection: ydb_dbapi.Connection) -> ydb.RetrySettings:
        return dialect.get_ydb_retry_settings(dbapi_connection)


class YqlDialect(StrCompileDialect):
    name = "yql"
    driver = "ydb"

    supports_alter = False
    max_identifier_length = 63
    supports_sane_rowcount = False
    supports_statement_cache = True

    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_smallserial = False
    supports_schemas = False
    supports_constraint_comments = False
    supports_json_type = True

    insert_returning = False
    update_returning = False
    delete_returning = False

    supports_sequences = False
    sequences_optional = False
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False

    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    default_paramstyle = "qmark"

    isolation_level = None

    preparer = YqlIdentifierPreparer
    statement_compiler = YqlCompiler
    ddl_compiler = YqlDDLCompiler
    type_compiler = YqlTypeCompiler
    colspecs = {
        sa.types.JSON: types.YqlJSON,
        sa.types.JSON.JSONPathType: types.YqlJSON.YqlJSONPathType,
        sa.types.Date: types.YqlDate,
        sa.types.DateTime: types.YqlTimestamp,  # Because YDB's DateTime doesn't store microseconds
        sa.types.DATETIME: types.YqlDateTime,
        sa.types.TIMESTAMP: types.YqlTimestamp,
        sa.types.DECIMAL: types.Decimal,
        sa.types.BINARY: types.Binary,
        sa.types.LargeBinary: types.Binary,
        sa.types.BLOB: types.Binary,
        sa.types.ARRAY: types.ListType,
    }

    connection_characteristics = util.immutabledict(
        {
            "isolation_level": characteristics.IsolationLevelCharacteristic(),
            "ydb_request_settings": YdbRequestSettingsCharacteristic(),
            "ydb_retry_settings": YdbRetrySettingsCharacteristic(),
        }
    )

    construct_arguments = [
        (
            sa.schema.Table,
            {
                "auto_partitioning_by_size": None,
                "auto_partitioning_by_load": None,
                "auto_partitioning_partition_size_mb": None,
                "auto_partitioning_min_partitions_count": None,
                "auto_partitioning_max_partitions_count": None,
                "uniform_partitions": None,
                "partition_at_keys": None,
            },
        ),
        (
            sa.schema.Index,
            {
                "async": False,
                "cover": [],
            },
        ),
    ]

    @classmethod
    def import_dbapi(cls: Any):
        return ydb_dbapi

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def __init__(
        self,
        json_serializer=None,
        json_deserializer=None,
        _add_declare_for_yql_stmt_vars=False,
        _statement_prefixes_list=None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer
        # NOTE: _add_declare_for_yql_stmt_vars is temporary and is soon to be removed.
        # no need in declare in yql statement here since ydb 24-1
        self._add_declare_for_yql_stmt_vars = _add_declare_for_yql_stmt_vars
        self._statement_prefixes = tuple(_statement_prefixes_list) if _statement_prefixes_list else ()

    def _describe_table(self, connection, table_name, schema=None) -> ydb.TableDescription:
        if schema is not None:
            raise ydb_dbapi.NotSupportedError("unsupported on non empty schema")

        qt = table_name if isinstance(table_name, str) else table_name.name
        raw_conn = connection.connection
        try:
            return raw_conn.describe(qt)
        except ydb_dbapi.DatabaseError as e:
            raise NoSuchTableError(qt) from e

    def get_view_names(self, connection, schema=None, **kw: Any):
        return []

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        table = self._describe_table(connection, table_name, schema)
        as_compatible = []
        for column in table.columns:
            col_type, nullable = _get_column_info(column.type)
            as_compatible.append(
                {
                    "name": column.name,
                    "type": col_type,
                    "nullable": nullable,
                    "default": None,
                }
            )

        return as_compatible

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema:
            raise ydb_dbapi.NotSupportedError("unsupported on non empty schema")

        raw_conn = connection.connection
        return raw_conn.get_table_names()

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kwargs):
        try:
            self._describe_table(connection, table_name, schema)
            return True
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        table = self._describe_table(connection, table_name, schema)
        return {"constrained_columns": table.primary_key, "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        # foreign keys unsupported
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        table = self._describe_table(connection, table_name, schema)
        indexes: list[ydb.TableIndex] = table.indexes
        if OLD_SA:
            sa_indexes: list[dict] = []
            for index in indexes:
                sa_indexes.append(
                    {
                        "name": index.name,
                        "column_names": index.index_columns,
                        "unique": False,
                        "dialect_options": {
                            "ydb_async": False,  # TODO After https://github.com/ydb-platform/ydb-python-sdk/issues/351
                            "ydb_cover": [],  # TODO After https://github.com/ydb-platform/ydb-python-sdk/issues/409
                        },
                    }
                )
            return sa_indexes

        sa_indexes: list[sa.engine.interfaces.ReflectedIndex] = []
        for index in indexes:
            sa_indexes.append(
                sa.engine.interfaces.ReflectedIndex(
                    name=index.name,
                    column_names=index.index_columns,
                    unique=False,
                    dialect_options={
                        "ydb_async": False,  # TODO After https://github.com/ydb-platform/ydb-python-sdk/issues/351
                        "ydb_cover": [],  # TODO After https://github.com/ydb-platform/ydb-python-sdk/issues/409
                    },
                )
            )
        return sa_indexes

    def set_isolation_level(self, dbapi_connection: ydb_dbapi.Connection, level: str) -> None:
        dbapi_connection.set_isolation_level(level)

    def get_default_isolation_level(self, dbapi_conn: ydb_dbapi.Connection) -> str:
        return ydb_dbapi.IsolationLevel.AUTOCOMMIT

    def get_isolation_level(self, dbapi_connection: ydb_dbapi.Connection) -> str:
        return dbapi_connection.get_isolation_level()

    def set_ydb_request_settings(
        self,
        dbapi_connection: ydb_dbapi.Connection,
        value: ydb.BaseRequestSettings,
    ) -> None:
        dbapi_connection.set_ydb_request_settings(value)

    def reset_ydb_request_settings(self, dbapi_connection: ydb_dbapi.Connection):
        self.set_ydb_request_settings(dbapi_connection, ydb.BaseRequestSettings())

    def get_ydb_request_settings(self, dbapi_connection: ydb_dbapi.Connection) -> ydb.BaseRequestSettings:
        return dbapi_connection.get_ydb_request_settings()

    def set_ydb_retry_settings(
        self,
        dbapi_connection: ydb_dbapi.Connection,
        value: ydb.RetrySettings,
    ) -> None:
        dbapi_connection.set_ydb_retry_settings(value)

    def reset_ydb_retry_settings(self, dbapi_connection: ydb_dbapi.Connection):
        self.set_ydb_retry_settings(dbapi_connection, ydb.RetrySettings())

    def get_ydb_retry_settings(self, dbapi_connection: ydb_dbapi.Connection) -> ydb.RetrySettings:
        return dbapi_connection.get_ydb_retry_settings()

    def create_connect_args(self, url):
        args, kwargs = super().create_connect_args(url)
        # YDB database name should start with '/'
        if "database" in kwargs:
            if not kwargs["database"].startswith("/"):
                kwargs["database"] = "/" + kwargs["database"]

        return [args, kwargs]

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

    def do_begin(self, dbapi_connection: ydb_dbapi.Connection) -> None:
        dbapi_connection.begin()

    def do_rollback(self, dbapi_connection: ydb_dbapi.Connection) -> None:
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection: ydb_dbapi.Connection) -> None:
        dbapi_connection.commit()

    def _handle_column_name(self, variable):
        return "`" + variable + "`"

    def _format_variables(
        self,
        statement: str,
        parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]],
        execute_many: bool,
    ) -> Tuple[str, Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]]:
        formatted_statement = statement
        formatted_parameters = None

        if parameters:
            if execute_many:
                parameters_sequence: Sequence[Mapping[str, Any]] = parameters
                variable_names = set()
                formatted_parameters = []
                for i in range(len(parameters_sequence)):
                    variable_names.update(set(parameters_sequence[i].keys()))
                    formatted_parameters.append({f"${k}": v for k, v in parameters_sequence[i].items()})
            else:
                variable_names = set(parameters.keys())
                formatted_parameters = {f"${k}": v for k, v in parameters.items()}

            formatted_variable_names = {
                variable_name: f"${self._handle_column_name(variable_name)}" for variable_name in variable_names
            }
            formatted_statement = formatted_statement % formatted_variable_names

        formatted_statement = formatted_statement.replace("%%", "%")
        return formatted_statement, formatted_parameters

    def _add_declare_for_yql_stmt_vars_impl(self, statement, parameters_types):
        declarations = "\n".join(
            [
                f"DECLARE $`{param_name[1:] if param_name.startswith('$') else param_name}` as {str(param_type)};"
                for param_name, param_type in parameters_types.items()
            ]
        )
        return f"{declarations}\n{statement}"

    def _apply_statement_prefixes_impl(self, statement: str) -> str:
        if not self._statement_prefixes:
            return statement
        prefixes = "\n".join(self._statement_prefixes) + "\n"
        return f"{prefixes}{statement}"

    def __merge_parameters_values_and_types(
        self, values: Mapping[str, Any], types: Mapping[str, Any], execute_many: bool
    ) -> Sequence[Mapping[str, ydb.TypedValue]]:
        if isinstance(values, collections.abc.Mapping):
            values = [values]

        result_list = []
        for value_map in values:
            result = {}
            for key in value_map.keys():
                if key in types:
                    result[key] = ydb.TypedValue(value_map[key], types[key])
                else:
                    result[key] = value_map[key]
            result_list.append(result)
        return result_list if execute_many else result_list[0]

    def _prepare_ydb_query(
        self,
        statement: str,
        context: Optional[DefaultExecutionContext] = None,
        parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]] = None,
        execute_many: bool = False,
    ) -> Tuple[Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]]:
        is_ddl = context.isddl if context is not None else False

        if not is_ddl and parameters:
            parameters_types = context.compiled.get_bind_types(parameters)
            if parameters_types != {}:
                parameters = self.__merge_parameters_values_and_types(parameters, parameters_types, execute_many)
            statement, parameters = self._format_variables(statement, parameters, execute_many)
            if self._add_declare_for_yql_stmt_vars:
                statement = self._add_declare_for_yql_stmt_vars_impl(statement, parameters_types)
            statement = self._apply_statement_prefixes_impl(statement)
            return statement, parameters

        statement, parameters = self._format_variables(statement, parameters, execute_many)
        statement = self._apply_statement_prefixes_impl(statement)
        return statement, parameters

    def do_ping(self, dbapi_connection: ydb_dbapi.Connection) -> bool:
        cursor = dbapi_connection.cursor()
        statement, _ = self._prepare_ydb_query(self._dialect_specific_select_one)
        try:
            cursor.execute(statement)
        finally:
            cursor.close()
        return True

    def do_executemany(
        self,
        cursor: ydb_dbapi.Cursor,
        statement: str,
        parameters: Optional[Sequence[Mapping[str, Any]]],
        context: Optional[DefaultExecutionContext] = None,
    ) -> None:
        operation, parameters = self._prepare_ydb_query(statement, context, parameters, execute_many=True)
        cursor.executemany(operation, parameters)

    def do_execute(
        self,
        cursor: ydb_dbapi.Cursor,
        statement: str,
        parameters: Optional[Mapping[str, Any]] = None,
        context: Optional[DefaultExecutionContext] = None,
    ) -> None:
        operation, parameters = self._prepare_ydb_query(statement, context, parameters, execute_many=False)
        is_ddl = context.isddl if context is not None else False
        if is_ddl:
            cursor.execute_scheme(operation, parameters)
        else:
            cursor.execute(operation, parameters)


class AsyncYqlDialect(YqlDialect):
    driver = "ydb_async"
    is_async = True
    supports_statement_cache = True

    def connect(self, *cargs, **cparams):
        return AdaptedAsyncConnection(util.await_only(self.dbapi.async_connect(*cargs, **cparams)))
