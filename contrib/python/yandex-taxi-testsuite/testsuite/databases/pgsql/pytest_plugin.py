import collections
import collections.abc
import concurrent.futures
import contextlib
import re
import typing

import pytest

from . import connection, control, discover, exceptions, service, utils

DB_FILE_RE_PATTERN = re.compile(r'/pg_(?P<pg_db_alias>\w+)(/?\w*)\.sql$')


class ServiceLocalConfig(collections.abc.Mapping):
    def __init__(
        self,
        databases: list[discover.PgShardedDatabase],
        pgsql_control: control.PgControl,
        cleanup_exclude_tables: frozenset[str],
    ):
        self._initialized = False
        self._pgsql_control = pgsql_control
        self._databases = databases
        self._shard_connections = {
            shard.pretty_name: pgsql_control.get_connection_cached(
                shard.dbname,
            )
            for db in self._databases
            for shard in db.shards
        }
        self._cleanup_exclude_tables = cleanup_exclude_tables

    def __len__(self) -> int:
        return len(self._shard_connections)

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._shard_connections)

    def __getitem__(self, dbname: str) -> connection.PgConnectionInfo:
        """Get
        :py:class:`testsuite.databases.pgsql.connection.PgConnectionInfo`
        instance by database name
        """
        return self._shard_connections[dbname].conninfo

    def initialize(
        self, parallel_init: bool
    ) -> dict[str, control.ConnectionWrapper]:
        if self._initialized:
            return self._shard_connections

        if self._databases:
            self._pgsql_control.initialize()

        def init_database(db):
            self._pgsql_control.initialize_sharded_db(db)

            for shard in db.shards:
                self._shard_connections[shard.pretty_name].initialize(
                    self._cleanup_exclude_tables
                )

        if parallel_init:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                init_db_futures = [
                    executor.submit(init_database, db) for db in self._databases
                ]
                for future in init_db_futures:
                    future.result()
        else:
            for database in self._databases:
                init_database(database)

        self._initialized = True
        return self._shard_connections


def pytest_addoption(parser):
    """
    :param parser: pytest's argument parser
    """
    group = parser.getgroup('postgresql')
    group.addoption('--postgresql', help='PostgreSQL connection string')
    group.addoption(
        '--no-postgresql',
        help='Disable use of PostgreSQL',
        action='store_true',
    )
    group.addoption(
        '--postgresql-keep-existing-db',
        action='store_true',
        help=(
            'Keep existing databases with up-to-date schema. By default '
            'testsuite will drop and create anew any existing database when '
            'initializing databases.'
        ),
    )


def pytest_report_header(config):
    conninfo = _get_connection_info(config)
    return [f'PostgreSQL: {conninfo.get_uri()}']


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'pgsql: per-test PostgreSQL initialization',
    )


def pytest_service_register(register_service):
    register_service('postgresql', service.create_pgsql_service)


@pytest.fixture(scope='session')
def pgsql_cleanup_exclude_tables() -> frozenset[str]:
    return frozenset()


@pytest.fixture
def pgsql(_pgsql, pgsql_apply) -> dict[str, control.PgDatabaseWrapper]:
    """
    Returns str to
    :py:class:`testsuite.databases.pgsql.control.PgDatabaseWrapper` dictionary

    Example usage:

    .. code-block:: python

      def test_pg(pgsql):
          cursor = pgsql['example_db'].cursor()
          cursor.execute('SELECT ... FROM ...WHERE ...')
          assert list(cusror) == [...]
    """
    return {
        dbname: control.PgDatabaseWrapper(connection)
        for dbname, connection in _pgsql.items()
    }


@pytest.fixture(scope='session')
def pgsql_local_create(
    _pgsql_control,
    pgsql_cleanup_exclude_tables,
) -> typing.Callable[
    [list[discover.PgShardedDatabase]],
    ServiceLocalConfig,
]:
    """Creates pgsql configuration.

    :param databases: List of databases.
    :returns: :py:class:`ServiceLocalConfig` instance.
    """

    def _pgsql_local_create(databases):
        return ServiceLocalConfig(
            databases,
            _pgsql_control,
            pgsql_cleanup_exclude_tables,
        )

    return _pgsql_local_create


@pytest.fixture(scope='session')
def pgsql_disabled(pytestconfig) -> bool:
    return pytestconfig.option.no_postgresql


@pytest.fixture
def pgsql_local(pgsql_local_create) -> ServiceLocalConfig:
    """Configures local pgsql instance.

    :returns: :py:class:`ServiceLocalConfig` instance.

    In order to use pgsql fixture you have to override pgsql_local()
    in your local conftest.py file, example:

    .. code-block:: python

        @pytest.fixture(scope='session')
        def pgsql_local(pgsql_local_create):
            databases = discover.find_schemas(
                'service_name', [PG_SCHEMAS_PATH])
            return pgsql_local_create(list(databases.values()))

    Sometimes it is desirable to have tests-only database, maybe used in one
    particular test or tests group. This can be achieved by by overriding
    ``pgsql_local`` fixture in your test file:

    .. code-block:: python

        @pytest.fixture
        def pgsql_local(pgsql_local_create):
            databases = discover.find_schemas(
                'testsuite', [pathlib.Path('custom/pgsql/schema/path')])
            return pgsql_local_create(list(databases.values()))

    ``pgsql_local`` provides access to PostgreSQL connection parameters:

    .. code-block:: python

        def get_custom_connection_string(pgsql_local):
            conninfo = pgsql_local['database_name']
            custom_dsn: str = conninfo.replace(options='-c opt=val').get_dsn()
            return custom_dsn
    """
    return pgsql_local_create([])


@pytest.fixture(scope='session')
def pgsql_parallelization_enabled():
    return True


@pytest.fixture
def _pgsql(
    _pgsql_service,
    _pgsql_control,
    pgsql_local,
    pgsql_cleanup_exclude_tables,
    pgsql_disabled: bool,
    pgsql_parallelization_enabled: bool,
) -> dict[str, control.ConnectionWrapper]:
    if pgsql_disabled:
        pgsql_local = ServiceLocalConfig(
            [],
            _pgsql_control,
            pgsql_cleanup_exclude_tables,
        )
    return pgsql_local.initialize(parallel_init=pgsql_parallelization_enabled)


@pytest.fixture(scope='session')
def pgsql_background_truncate_enabled():
    return True


@pytest.fixture
def _pgsql_apply_queries(
    request, _pgsql: ServiceLocalConfig, _pgsql_query_loader
) -> dict[str, list[control.PgQuery]]:
    def pgsql_default_queries(dbname):
        return [
            *_pgsql_query_loader.load(
                f'pg_{dbname}.sql',
                'pgsql.default_queries',
                missing_ok=True,
            ),
            *_pgsql_query_loader.loaddir(
                f'pg_{dbname}',
                'pgsql.default_queries',
                missing_ok=True,
            ),
        ]

    def pgsql_mark(dbname, files=(), directories=(), queries=()):
        result_queries = []

        for path in files:
            result_queries += _pgsql_query_loader.load(path, 'mark.pgsql.files')
        for path in directories:
            result_queries += _pgsql_query_loader.loaddir(
                path,
                'mark.pgsql.directories',
            )
        for query in queries:
            queries_str: typing.Iterable = []
            if isinstance(query, str):
                queries_str = [query]
            elif isinstance(query, (list, tuple)):
                queries_str = query
            else:
                raise exceptions.PostgresqlError(
                    f'sql queries of type {type(query)} are not supported',
                )
            for query_str in queries_str:
                result_queries.append(
                    control.PgQuery(
                        body=query_str,
                        source='mark.pgsql.queries',
                        path=None,
                    ),
                )
        return dbname, result_queries

    overrides: typing.DefaultDict[
        str,
        list[control.PgQuery],
    ] = collections.defaultdict(list)
    for mark in request.node.iter_markers('pgsql'):
        dbname, queries = pgsql_mark(*mark.args, **mark.kwargs)
        if dbname not in _pgsql:
            raise exceptions.PostgresqlError(
                'Unknown database {}'.format(dbname)
            )
        overrides[dbname].extend(queries)

    queries = {}

    for dbname in _pgsql.keys():
        queries[dbname] = overrides.get(dbname, pgsql_default_queries(dbname))

    return queries


@pytest.fixture
def pgsql_apply(
    _pgsql: ServiceLocalConfig,
    load,
    pgsql_background_truncate_enabled: bool,
    pgsql_parallelization_enabled: bool,
    _pgsql_apply_queries,
) -> None:
    """Initialize PostgreSQL database with data.

    By default pg_${DBNAME}.sql and pg_${DBNAME}/*.sql files are used
    to fill PostgreSQL databases.

    Use pytest.mark.pgsql to change this behaviour:

    @pytest.mark.pgsql(
        'foo@0',
        files=[
            'pg_foo@0_alternative.sql'
        ],
        directories=[
            'pg_foo@0_alternative_dir'
        ],
        queries=[
          'INSERT INTO foo VALUES (1, 2, 3, 4)',
        ]
    )
    """

    if pgsql_parallelization_enabled:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            db_apply_queries_future = []
            for dbname, pg_db in _pgsql.items():
                db_apply_queries_future.append(
                    executor.submit(
                        pg_db.apply_queries, _pgsql_apply_queries[dbname]
                    )
                )

            for future in db_apply_queries_future:
                future.result()

    else:
        for dbname, pg_db in _pgsql.items():
            pg_db.apply_queries(_pgsql_apply_queries[dbname])

    yield

    if pgsql_background_truncate_enabled:
        for pg_db in _pgsql.values():
            pg_db.schedule_truncation()


@pytest.fixture
def _pgsql_query_loader(get_file_path, get_directory_path, mockserver_info):
    def substitute_mockserver(str_val: str):
        return str_val.replace(
            '$mockserver',
            f'http://{mockserver_info.host}:{mockserver_info.port}',
        )

    def load_pg_file(path, source):
        query = substitute_mockserver(path.read_text())
        return control.PgQuery(body=query, source=source, path=str(path))

    class Loader:
        @staticmethod
        def load(path, source, missing_ok=False):
            path = get_file_path(path, missing_ok=missing_ok)
            if not path:
                return []
            return [load_pg_file(path, source)]

        @staticmethod
        def loaddir(directory, source, missing_ok=False):
            result = []
            directory = get_directory_path(directory, missing_ok=missing_ok)
            if not directory:
                return []
            for path in utils.scan_sql_directory(directory):
                result.append(load_pg_file(path, source))
            return result

    return Loader()


@pytest.fixture
def _pgsql_service(
    pytestconfig,
    pgsql_disabled: bool,
    ensure_service_started,
    pgsql_local: ServiceLocalConfig,
    _pgsql_service_settings,
) -> None:
    if (
        not pgsql_disabled
        and pgsql_local
        and not pytestconfig.option.postgresql
    ):
        ensure_service_started('postgresql', settings=_pgsql_service_settings)


@pytest.fixture(scope='session')
def _pgsql_control(pytestconfig, pgsql_disabled: bool):
    if pgsql_disabled:
        return {}
    instance = control.PgControl(
        _get_connection_info(pytestconfig),
        verbose=pytestconfig.option.verbose,
        skip_applied_schemas=(
            pytestconfig.option.postgresql_keep_existing_db
            or pytestconfig.option.service_wait
        ),
    )
    with contextlib.closing(instance):
        yield instance


@pytest.fixture(scope='session')
def _pgsql_service_settings() -> service.ServiceSettings:
    return service.get_service_settings()


@pytest.fixture(scope='session')
def _pgsql_conninfo(
    request,
    _pgsql_service_settings,
) -> connection.PgConnectionInfo:
    return _get_connection_info(request.config)


def _get_connection_info(config):
    connstr = config.option.postgresql
    if connstr:
        return connection.parse_connection_string(connstr)
    settings = service.get_service_settings()
    return settings.get_conninfo()
