import json

import pytest
import redis as redisdb

from . import service


def pytest_addoption(parser):
    group = parser.getgroup('redis')
    group.addoption('--redis-host', help='Redis host')
    group.addoption('--redis-master-port', type=int, help='Redis master port')
    group.addoption(
        '--redis-sentinel-port',
        type=int,
        help='Redis sentinel port',
    )
    group.addoption(
        '--no-redis',
        help='Do not fill redis storage',
        action='store_true',
    )


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'redis_store: per-test redis initialization',
    )
    config.addinivalue_line(
        'markers',
        'redis_cluster_store: per-test redis cluster initialization',
    )
    config.addinivalue_line(
        'markers',
        'redis_standalone_store: per-test standalone redis initialization',
    )


def pytest_service_register(register_service):
    register_service('redis', service.create_redis_service)
    register_service('redis-cluster', service.create_cluster_redis_service)
    register_service(
        'redis-standalone', service.create_standalone_redis_service
    )


@pytest.fixture(scope='session')
def redis_service(
    pytestconfig,
    ensure_service_started,
    _redis_service_settings,
):
    if not pytestconfig.option.no_redis and not pytestconfig.option.redis_host:
        ensure_service_started('redis', settings=_redis_service_settings)


@pytest.fixture(scope='session')
def redis_cluster_service(
    pytestconfig, ensure_service_started, _redis_cluster_service_settings
):
    if not pytestconfig.option.no_redis and not pytestconfig.option.redis_host:
        ensure_service_started(
            'redis-cluster', settings=_redis_cluster_service_settings
        )


@pytest.fixture(scope='session')
async def redis_standalone_service(
    pytestconfig, ensure_service_started, _redis_standalone_service_settings
):
    if not pytestconfig.option.no_redis and not pytestconfig.option.redis_host:
        ensure_service_started(
            'redis-standalone', settings=_redis_standalone_service_settings
        )


@pytest.fixture
def redis_store(
    pytestconfig,
    _redis_store,
    _redis_execute_commands_from_file,
):
    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = _redis_store

    try:
        _redis_execute_commands_from_file('redis_store', redis_db)
        yield redis_db
    finally:
        redis_db.flushall()


@pytest.fixture
def redis_sentinel(
    pytestconfig,
    request,
    load_json,
    redis_service,
    redis_sentinels,
):
    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = redisdb.StrictRedis(
        host=redis_sentinels[0]['host'],
        port=redis_sentinels[0]['port'],
    )

    yield redis_db


@pytest.fixture
def redis_cluster_store(
    pytestconfig,
    _redis_cluster_store,
    _redis_execute_commands_from_file,
):
    def _flush_all(redis_db):
        slot_infos = redis_db.cluster_slots()
        nodes = redis_db.get_primaries()
        redis_db.flushall(target_nodes=nodes)
        redis_db.wait(1, 10, target_nodes=nodes)

    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = _redis_cluster_store

    try:
        _redis_execute_commands_from_file('redis_cluster_store', redis_db)
        yield redis_db
    finally:
        _flush_all(redis_db)


@pytest.fixture(scope='session')
def _redis_masters(pytestconfig, _redis_service_settings):
    if pytestconfig.option.redis_host:
        # external Redis instance
        return [
            {
                'host': pytestconfig.option.redis_host,
                'port': (
                    pytestconfig.option.redis_master_port
                    or _redis_service_settings.master_ports[0]
                ),
            },
        ]
    return [
        {'host': _redis_service_settings.host, 'port': port}
        for port in _redis_service_settings.master_ports
    ]


@pytest.fixture(scope='session')
def redis_sentinels(pytestconfig, _redis_service_settings):
    if pytestconfig.option.redis_host:
        # external Redis instance
        return [
            {
                'host': pytestconfig.option.redis_host,
                'port': (
                    pytestconfig.option.redis_sentinel_port
                    or _redis_service_settings.sentinel_port
                ),
            },
        ]
    return [
        {
            'host': _redis_service_settings.host,
            'port': _redis_service_settings.sentinel_port,
        },
    ]


@pytest.fixture(scope='session')
def redis_cluster_nodes(_redis_cluster_service_settings):
    return [
        {
            'host': _redis_cluster_service_settings.host,
            'port': port,
        }
        for port in _redis_cluster_service_settings.cluster_ports
    ]


@pytest.fixture(scope='session')
def redis_standalone_node(_redis_standalone_service_settings):
    return {
        'host': _redis_standalone_service_settings.host,
        'port': _redis_standalone_service_settings.port,
    }


@pytest.fixture(scope='session')
def redis_cluster_replicas(_redis_cluster_service_settings):
    return _redis_cluster_service_settings.cluster_replicas


@pytest.fixture(scope='session')
def _redis_service_settings():
    return service.get_service_settings()


@pytest.fixture(scope='session')
def _redis_cluster_service_settings():
    return service.get_cluster_service_settings()


@pytest.fixture(scope='session')
def _redis_standalone_service_settings():
    return service.get_standalone_service_settings()


def _json_object_hook(dct):
    if '$json' in dct:
        return json.dumps(dct['$json'])
    return dct


@pytest.fixture(scope='session')
def _redis_store(
    pytestconfig,
    redis_service,
    _redis_masters,
):
    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = redisdb.StrictRedis(
        host=_redis_masters[0]['host'],
        port=_redis_masters[0]['port'],
    )

    yield redis_db


# creating RedisCluster is costly, so we do it once at session scope
@pytest.fixture(scope='session')
def _redis_cluster_store(
    pytestconfig,
    redis_cluster_service,
    redis_cluster_nodes,
):
    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = redisdb.RedisCluster(  # type: ignore[abstract]
        host=redis_cluster_nodes[0]['host'],
        port=redis_cluster_nodes[0]['port'],
    )

    yield redis_db


@pytest.fixture
def redis_standalone_store(
    pytestconfig,
    redis_standalone_service,
    redis_standalone_node,
    _redis_execute_commands_from_file,
):
    if pytestconfig.option.no_redis:
        yield
        return

    redis_db = redisdb.StrictRedis(
        host=redis_standalone_node['host'],
        port=redis_standalone_node['port'],
    )

    try:
        _redis_execute_commands_from_file('redis_standalone_store', redis_db)
        yield redis_db
    finally:
        redis_db.flushall()


@pytest.fixture
def _redis_execute_commands_from_file(request, load_json):
    def _execute_commands(markers, redis_db):
        redis_commands = []

        for mark in request.node.iter_markers(markers):
            store_file = mark.kwargs.get('file')
            if store_file is not None:
                redis_commands_from_file = load_json(
                    '%s.json' % store_file,
                    object_hook=_json_object_hook,
                )
                redis_commands.extend(redis_commands_from_file)

            if mark.args:
                redis_commands.extend(mark.args)

        for redis_command in redis_commands:
            func = getattr(redis_db, redis_command[0])
            func(*redis_command[1:])

    return _execute_commands
