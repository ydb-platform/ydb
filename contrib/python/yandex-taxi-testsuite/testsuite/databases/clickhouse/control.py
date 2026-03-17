import dataclasses
import logging
import pathlib

import clickhouse_driver

from . import classes

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ClickhouseQuery:
    body: str
    source: str
    path: str | None


class ConnectionCache:
    def __init__(self, conn_info: classes.ConnectionInfo):
        self._conn_info = conn_info
        self._cache: dict[str, clickhouse_driver.Client] = {}
        self._master_connection = None

    def get_master_connection(self):
        return self.get_connection('default')

    def get_connection(self, dbname: str) -> clickhouse_driver.Client:
        if dbname not in self._cache:
            self._cache[dbname] = self._create_connection(dbname)
        return self._cache[dbname]

    def get_connection_info(self, dbname: str) -> classes.ConnectionInfo:
        return self._conn_info.replace(dbname=dbname)

    def _create_connection(self, dbname: str):
        return self._connect(self.get_connection_info(dbname))

    def _connect(self, conn_info: classes.ConnectionInfo):
        return clickhouse_driver.Client(
            host=conn_info.host,
            port=conn_info.tcp_port,
            database=conn_info.dbname,
        )


class DatabasesState:
    _migrations_run: set[tuple[str, pathlib.Path]]
    _initialized: set[str]

    def __init__(self, connections: ConnectionCache, verbose: bool = False):
        self._connections = connections
        self._verbose = verbose
        self._migrations_run = set()
        self._initialized = set()

    def get_connection(
        self,
        dbname: str,
        create_db: bool = True,
    ) -> clickhouse_driver.Client:
        if dbname not in self._initialized:
            if create_db:
                self._init_db(dbname)
            self._initialized.add(dbname)
        return self._connections.get_connection(dbname)

    def run_migration(self, dbname: str, path: pathlib.Path):
        key = dbname, path
        if key in self._migrations_run:
            return
        logger.debug(
            'Running clickhouse-sql script %s against database %s',
            path,
            dbname,
        )
        conn = self.get_connection(dbname)
        conn.execute(path.read_text())
        self._migrations_run.add(key)

    def _init_db(self, dbname: str):
        conn = self._connections.get_master_connection()
        conn.execute(f'DROP DATABASE IF EXISTS `{dbname}`')
        conn.execute(f'CREATE DATABASE `{dbname}`')


class Control:
    def __init__(
        self,
        databases: classes.DatabasesDict,
        state: DatabasesState,
    ):
        self._databases = databases
        self._state = state

    def get_connections(self):
        return {
            alias: self._state.get_connection(dbconfig.dbname)
            for alias, dbconfig in self._databases.items()
        }

    def run_migrations(self):
        for dbconfig in self._databases.values():
            self._run_database_migrations(dbconfig)

    def _run_database_migrations(self, dbconfig: classes.DatabaseConfig):
        for path in dbconfig.migrations:
            self._state.run_migration(dbconfig.dbname, path)


def _get_db_tables_list(connection: clickhouse_driver.Client):
    return connection.execute('SHOW TABLES')


def apply_queries(
    connection: clickhouse_driver.Client,
    queries: list[ClickhouseQuery],
):
    tables = _get_db_tables_list(connection)
    if tables:
        for (table,) in tables:
            connection.execute(f'TRUNCATE TABLE `{table}`')

    for query in queries:
        try:
            connection.execute(query.body)
        except Exception as exc:
            error_message = (
                f'ClickHouse apply query error\nQuery from {query.source}\n'
            )
            if query.path:
                error_message += f'File path: {query.path}\n'
            error_message += '\n' + str(exc)
            raise RuntimeError(error_message)
