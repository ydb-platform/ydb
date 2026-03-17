import contextlib
import dataclasses
import multiprocessing.pool
import pathlib
import pprint
import random
import re

import pymongo
import pymongo.collection
import pymongo.errors
import pytest
from bson import json_util

from testsuite import types, utils

from . import connection, ensure_db_indexes, mongo_schema, service

# pylint: disable=too-many-statements

DB_FILE_RE_PATTERN = re.compile(r'^db_(?P<mongo_db_alias>\w+)\.json$')
JSON_OPTIONS = json_util.JSONOptions(tz_aware=False)
MONGO_OBJECT_HOOKS = (
    '$binary',
    '$code',
    '$date',
    '$dbPointer',
    '$maxKey',
    '$minKey',
    '$numberDecimal',
    '$numberDouble',
    '$numberInt',
    '$numberLong',
    '$oid',
    '$ref',
    '$regex',
    '$regularExpression',
    '$symbol',
    '$timestamp',
    '$undefined',
    '$uuid',
)


class BaseError(Exception):
    """Base testsuite error"""


class UnknownCollectionError(BaseError):
    pass


class CollectionWrapper:
    def __init__(self, collections):
        # TODO: deprecate collection as attribute
        for alias, collection in collections.items():
            setattr(self, alias, collection)
        self._collections = collections.copy()
        self._aliases = tuple(collections.keys())

    def __getitem__(self, alias: str) -> pymongo.collection.Collection:
        return self._collections[alias]

    def __contains__(self, alias: str) -> bool:
        return alias in self._collections

    def get_aliases(self) -> tuple[str]:
        return self._aliases


class CollectionWrapperFactory:
    def __init__(self, connection_info: connection.ConnectionInfo):
        self._connection_info = connection_info

    @property
    def connection_string(self) -> str:
        return self._connection_info.get_uri()

    @utils.cached_property
    def client(self) -> pymongo.MongoClient:
        return pymongo.MongoClient(self.connection_string)

    def create_collection_wrapper(
        self,
        collection_names,
        mongodb_settings,
    ) -> CollectionWrapper:
        collections = {}
        for name in collection_names:
            if name not in mongodb_settings:
                raise UnknownCollectionError(
                    f'Missing collection {name} in mongodb_settings fixture',
                )
            # pylint: disable=unsubscriptable-object
            settings = mongodb_settings[name]['settings']
            database = self.client[settings['database']]
            collections[name] = database[settings['collection']]
        return CollectionWrapper(collections)


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'noshuffledb: disable data set shuffle for marked test',
    )
    config.addinivalue_line(
        'markers',
        'filldb: specify mongo static file suffix',
    )
    config.addinivalue_line(
        'markers',
        'mongodb_collections: override mongo collections list',
    )


def pytest_addoption(parser):
    """
    :param parser: pytest's argument parser
    """
    group = parser.getgroup('mongo')
    group.addoption('--mongo', help='Mongo connection string.')
    group.addoption(
        '--no-indexes',
        action='store_true',
        help='Disable index creation.',
    )
    group.addoption(
        '--no-shuffle-db',
        action='store_true',
        help='Disable fixture data shuffle.',
    )
    group.addoption(
        '--no-sharding',
        action='store_true',
        help='Disable collections sharding.',
    )
    group.addoption(
        '--no-mongo',
        help='Disable mongo startup',
        action='store_true',
    )
    parser.addini(
        'mongo-retry-writes',
        type='bool',
        default=False,
        help=(
            "Controls value of 'retryWrites' parameter of mongo connection "
            'string.'
        ),
    )


def pytest_report_header(config):
    conninfo = _get_connection_info(config)
    return [f'Mongo: {conninfo.get_uri()}']


def pytest_service_register(register_service):
    register_service('mongo', service.create_mongo_service)


def pytest_register_object_hooks():
    return {key: _mongo_object_hook for key in MONGO_OBJECT_HOOKS}


@pytest.fixture
def mongodb(
    mongodb_init,
    _mongodb_local: CollectionWrapper,
) -> CollectionWrapper:
    return _mongodb_local


@pytest.fixture
def mongo_connections(
    mongodb_settings,
    mongo_connection_info,
    mongo_extra_connections,
    _mongo_local_collections,
) -> dict[str, str]:
    mongo_connection_uri = mongo_connection_info.get_uri()
    return {
        **{
            mongodb_settings[name]['settings'][
                'connection'
            ]: mongo_connection_uri
            for name in _mongo_local_collections
        },
        **{
            extra_conn: mongo_connection_uri
            for extra_conn in mongo_extra_connections
        },
    }


@pytest.fixture
def mongo_extra_connections() -> tuple[str, ...]:
    """
    Override this if you need to access mongo connections besides those
    defined in mongo_connections fixture
    """
    return ()


@pytest.fixture(scope='session')
def mongo_connection_info(
    pytestconfig,
) -> connection.ConnectionInfo:
    return _get_connection_info(pytestconfig)


@pytest.fixture
def mongodb_settings(
    mongo_schema_directory,
    mongo_schema_extra_directories,
    _mongo_schema_cache,
) -> mongo_schema.MongoSchemas:
    return mongo_schema.MongoSchemas(
        _mongo_schema_cache,
        (mongo_schema_directory, *mongo_schema_extra_directories),
    )


@pytest.fixture
def mongodb_collections(mongodb_settings) -> tuple[str, ...]:
    """
    Override this to enable access to named collections within test module

    Returns all available collections by default.
    """
    return tuple(mongodb_settings.keys())


@pytest.fixture(scope='session')
def mongo_schema_extra_directories() -> tuple[str, ...]:
    """
    Override to use collection schemas besides those defined by
    ``mongo_schema_directory`` fixture
    """
    return ()


@pytest.fixture(scope='session')
def _mongo_indexes_ensured() -> set[str]:
    return set()


@pytest.fixture
def _mongo_service(
    pytestconfig,
    ensure_service_started,
    _mongodb_local,
    _mongo_service_settings,
) -> None:
    aliases = _mongodb_local.get_aliases()
    if (
        aliases
        and not pytestconfig.option.mongo
        and not pytestconfig.option.no_mongo
    ):
        ensure_service_started('mongo', settings=_mongo_service_settings)


@pytest.fixture
def _mongo_create_indexes(
    _mongodb_local,
    mongodb_settings,
    pytestconfig,
    _mongo_indexes_ensured,
    _mongo_service,
) -> None:
    aliases = _mongodb_local.get_aliases()
    if not pytestconfig.option.no_indexes:
        _ensure_indexes = {}
        for alias in aliases:
            if (
                alias not in _mongo_indexes_ensured
                and alias in mongodb_settings
            ):
                _ensure_indexes[alias] = mongodb_settings[alias]
        if _ensure_indexes:
            sharding_enabled = not pytestconfig.option.no_sharding
            ensure_db_indexes.ensure_db_indexes(
                _mongodb_local,
                _ensure_indexes,
                sharding_enabled=sharding_enabled,
            )
            _mongo_indexes_ensured.update(_ensure_indexes)


@pytest.fixture(scope='session')
def _mongo_thread_pool() -> types.YieldFixture[
    multiprocessing.pool.ThreadPool,
]:
    pool = multiprocessing.pool.ThreadPool(processes=1)
    with contextlib.closing(pool):
        yield pool


@pytest.fixture
def _mongo_query_loader(load_json):
    def loader(filename, missing_ok=False):
        data = load_json(filename, missing_ok=missing_ok)
        if data is None:
            return []
        return data

    return loader


@pytest.fixture
def mongodb_init(
    request,
    verify_file_paths,
    static_dir: pathlib.Path,
    _mongodb_local,
    _mongo_thread_pool,
    _mongo_create_indexes,
    _mongo_query_loader,
) -> None:
    """Populate mongodb with fixture data."""

    if request.node.get_closest_marker('nofilldb'):
        return

    # Disable shuffle to make some buggy test work
    shuffle_enabled = (
        not request.config.option.no_shuffle_db
        and not request.node.get_closest_marker('noshuffledb')
    )
    aliases = {key: key for key in _mongodb_local.get_aliases()}
    requested = set()

    for marker in request.node.iter_markers('filldb'):
        for dbname, alias in marker.kwargs.items():
            if dbname not in aliases:
                raise UnknownCollectionError(
                    f'Unknown collection {dbname} requested'
                )
            if alias != 'default':
                aliases[dbname] = '{}_{}'.format(dbname, alias)
            requested.add(dbname)

    def _verify_db_alias(file_path: pathlib.Path) -> bool:
        if not _is_relevant_file(request, static_dir, file_path):
            return True
        match = DB_FILE_RE_PATTERN.search(file_path.name)
        if match:
            db_alias = match.group('mongo_db_alias')
            if db_alias not in aliases and not any(
                db_alias.startswith(alias + '_') for alias in aliases
            ):
                return False
        return True

    verify_file_paths(
        _verify_db_alias,
        check_name='mongo_db_aliases',
        text_at_fail='file has not valid mongo collection name alias '
        '(probably should add to service.yaml)',
    )

    def load_collection(params):
        dbname, alias = params
        try:
            col = getattr(_mongodb_local, dbname)
        except AttributeError:
            return

        docs = _mongo_query_loader(
            f'db_{alias}.json', missing_ok=dbname not in requested
        )

        if not docs and col.find_one({}, []) is None:
            return

        if shuffle_enabled:
            # Make sure there is no tests that depend on order of
            # documents in fixture file.
            random.shuffle(docs)

        try:
            col.bulk_write(
                [
                    pymongo.DeleteMany({}),
                    *(pymongo.InsertOne(doc) for doc in docs),
                ],
                ordered=True,
            )
        except pymongo.errors.BulkWriteError as bwe:
            pprint.pprint(bwe.details)
            raise

    pool_args = []
    for dbname, alias in aliases.items():
        pool_args.append((dbname, alias))

    _mongo_thread_pool.map(load_collection, pool_args)


@pytest.fixture
def _mongodb_local(
    mongodb_settings,
    _mongo_local_collections,
    _mongo_collection_wrapper_factory: CollectionWrapperFactory,
) -> CollectionWrapper:
    return _mongo_collection_wrapper_factory.create_collection_wrapper(
        _mongo_local_collections,
        mongodb_settings,
    )


@pytest.fixture(scope='session')
def _mongo_collection_wrapper_factory(
    mongo_connection_info: connection.ConnectionInfo,
) -> CollectionWrapperFactory:
    return CollectionWrapperFactory(mongo_connection_info)


@pytest.fixture
def _mongo_local_collections(request, mongodb_collections) -> set[str]:
    result = set(mongodb_collections)
    for marker in request.node.iter_markers('mongodb_collections'):
        result.update(marker.args)
    return result


@pytest.fixture(scope='session')
def _mongo_schema_cache() -> mongo_schema.MongoSchemaCache:
    return mongo_schema.MongoSchemaCache()


@pytest.fixture(scope='session')
def _mongo_service_settings(
    pytestconfig,
) -> service.ServiceSettings | None:
    if pytestconfig.option.mongo:
        return None
    return service.get_service_settings()


def _is_relevant_file(
    request,
    static_dir: pathlib.Path,
    file_path: pathlib.Path,
) -> bool:
    default_static_dir = static_dir / 'default'
    module_static_dir = static_dir / pathlib.Path(request.fspath).stem
    return _is_nested_path(file_path, default_static_dir) or _is_nested_path(
        file_path,
        module_static_dir,
    )


def _is_nested_path(parent: pathlib.Path, nested: pathlib.Path) -> bool:
    try:
        pathlib.PurePath(nested).relative_to(parent)
        return True
    except ValueError:
        return False


def _mongo_object_hook(doc):
    return json_util.object_hook(doc, JSON_OPTIONS)


def _get_connection_info(config):
    # External mongo instance
    if config.option.mongo:
        return connection.parse_connection_uri(config.option.mongo)
    service_settings = service.get_service_settings()
    connection_info = service_settings.get_connection_info()
    retry_writes = config.getini('mongo-retry-writes')
    return dataclasses.replace(connection_info, retry_writes=retry_writes)
