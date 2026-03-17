import contextlib
import dataclasses
import logging
import pathlib

import pymysql
import pymysql.constants

from testsuite.environment import shell
from testsuite.utils import cached_property

from . import classes, exceptions

logger = logging.getLogger(__name__)

MYSQL_HELPER = pathlib.Path(__file__).parent.joinpath('scripts/mysql-helper')


@dataclasses.dataclass(frozen=True)
class MysqlQuery:
    body: str
    source: str
    path: str | None


class ConnectionWrapper:
    """MySQL database connection wrapper."""

    def __init__(self, connection, conninfo, tables):
        self._connection = connection
        self._conninfo = conninfo
        self._tables: list[str] = tables

    @property
    def conninfo(self) -> classes.ConnectionInfo:
        """:py:class:`classes.ConnectionInfo` instance."""
        return self._conninfo

    def cursor(self, **kwargs) -> pymysql.cursors.Cursor:
        """Returns cursor instance."""
        return self._connection.cursor(**kwargs)

    def dict_cursor(self, **kwargs) -> pymysql.cursors.Cursor:
        """Return dictionary cursor, pymysql.cursors.DictCursor."""
        kwargs['cursor'] = pymysql.cursors.DictCursor
        return self.cursor(**kwargs)

    def commit(self) -> None:
        self._connection.commit()

    def _truncate_non_empty_tables(self) -> list[str] | None:
        cursor = self.cursor()
        if self._tables:
            with contextlib.closing(cursor):
                queries = []
                for table in self._tables:
                    queries.append(
                        f"select '{table}' as name, count(*) as c from {table}"
                    )
                subquery = ' union '.join(queries)
                query = f'select name from ({subquery}) tables where c>0;'
                cursor.execute(query)
                tables = cursor.fetchall()
                return [table for (table,) in tables]
        return None

    def apply_queries(
        self,
        queries: list[MysqlQuery],
        keep_tables: list[str] | None = None,
        truncate_non_empty: bool = False,
    ) -> None:
        if not keep_tables:
            keep_tables = []
        with self.cursor() as cursor:
            if truncate_non_empty:
                tables = self._truncate_non_empty_tables()
            else:
                tables = self._tables

            if tables:
                truncate_table_sql = []
                for table in tables:
                    if table not in keep_tables:
                        truncate_table_sql.append(f'truncate table {table};')
                truncate_tables_sql = ' '.join(truncate_table_sql)
                cursor.execute(
                    'set foreign_key_checks=0;'
                    f'{truncate_tables_sql}'
                    'set foreign_key_checks=1;',
                )
            for query in queries:
                try:
                    cursor.execute(query.body, args=[])
                except pymysql.Error as exc:
                    error_message = (
                        f'MySQL apply query error\nQuery from: {query.source}\n'
                    )
                    if query.path:
                        error_message += f'File path: {query.path}\n'
                    error_message += '\n' + str(exc)
                    raise exceptions.MysqlError(error_message)
        self.commit()


class ConnectionCache:
    def __init__(self, conninfo, verbose: bool = False):
        self._conninfo = conninfo
        self._cache: dict = {}
        self._master_connection = None

    def get_master_connection(self):
        if self._master_connection is None:
            self._master_connection = self._connect(self._conninfo)
        return self._master_connection

    def get_conninfo(self, dbname: str) -> classes.ConnectionInfo:
        return self._conninfo.replace(dbname=dbname)

    def get_connection(self, dbname):
        if dbname not in self._cache:
            self._cache[dbname] = self._create_connection(dbname)
        return self._cache[dbname]

    def _create_connection(self, dbname):
        return self._connect(self.get_conninfo(dbname))

    def _connect(self, conninfo: classes.ConnectionInfo):
        return pymysql.connect(
            host=conninfo.hostname,
            port=conninfo.port,
            user=conninfo.user,
            password=conninfo.password or '',
            database=conninfo.dbname,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
        )


class DatabasesState:
    _migrations_run: set[tuple[str, str]]
    _initialized: set[str]

    def __init__(self, connections: ConnectionCache, verbose: bool = False):
        self._need_save_tables = True
        self._connections = connections
        self._verbose = verbose
        self._migrations_run = set()
        self._initialized = set()
        self._tables: dict[str, list[str]] = dict()

    def get_connection(self, dbname: str, create_db: bool = True):
        if dbname not in self._initialized:
            if create_db:
                self._initdb(dbname)
            self._initialized.add(dbname)
        return self._connections.get_connection(dbname)

    def wrapper_for(self, dbname: str):
        return ConnectionWrapper(
            self._connections.get_connection(dbname),
            self._connections.get_conninfo(dbname),
            self._tables.get(dbname),
        )

    def run_migration(self, dbname: str, path: str):
        key = dbname, path
        if key in self._migrations_run:
            return
        logger.debug(
            'Running mysql script %s against database %s',
            path,
            dbname,
        )
        conninfo = self._connections.get_conninfo(dbname)
        _run_script(conninfo, ['-e', f'source {path}'], verbose=self._verbose)
        self._migrations_run.add(key)
        self._need_save_tables = True

    @cached_property
    def known_databases(self):
        connection = self._connections.get_master_connection()
        cursor = connection.cursor()
        cursor.execute('show databases')
        return {row[0] for row in cursor.fetchall()}

    def _initdb(self, dbname: str):
        connection = self._connections.get_master_connection()
        with connection.cursor() as cursor:
            if dbname in self.known_databases:
                cursor.execute(f'DROP DATABASE IF EXISTS `{dbname}`')
            cursor.execute(f'CREATE DATABASE `{dbname}`')
        connection.commit()
        self._initialized.add(dbname)

    def save_tables(self, dbname: str) -> None:
        if not self._need_save_tables:
            return
        connection = self._connections.get_connection(dbname)
        cursor = connection.cursor()
        with contextlib.closing(cursor):
            cursor.execute('show tables')
            self._tables[dbname] = [table for (table,) in cursor.fetchall()]
        self._need_save_tables = False


class Control:
    def __init__(
        self,
        databases: classes.DatabasesDict,
        state: DatabasesState,
    ):
        self._databases = databases
        self._state = state

    def get_wrappers(self):
        return {
            alias: self._state.wrapper_for(dbconfig.dbname)
            for alias, dbconfig in self._databases.items()
        }

    def run_migrations(self):
        for dbconfig in self._databases.values():
            self._run_database_migrations(dbconfig)

    def _run_database_migrations(self, dbconfig):
        self._state.get_connection(dbconfig.dbname, create_db=dbconfig.create)
        for path in dbconfig.migrations:
            self._state.run_migration(dbconfig.dbname, path)
        self._state.save_tables(dbconfig.dbname)


def _build_mysql_args(conninfo: classes.ConnectionInfo) -> list[str]:
    result = ['--protocol=tcp']
    if conninfo.hostname:
        result.append(f'--host={conninfo.hostname}')
    if conninfo.port:
        result.append(f'--port={conninfo.port}')
    if conninfo.user:
        result.append(f'--user={conninfo.user}')
    if conninfo.password:
        result.append(f'--password={conninfo.password}')
    if conninfo.dbname:
        result.append(f'--database={conninfo.dbname}')
    return result


def _run_script(
    conninfo: classes.ConnectionInfo,
    args: list[str],
    verbose: bool,
):
    command = [str(MYSQL_HELPER), *_build_mysql_args(conninfo), *args]
    shell.execute(command, verbose=verbose, command_alias='mysql/script')
