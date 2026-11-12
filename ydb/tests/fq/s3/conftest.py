import logging
import uuid

import pytest
import requests
import yatest.common
import library.python.port_manager
from ydb import convert

from library.recipes import common as recipes_common
from ydb.tests.fq.s3.s3_helpers import S3
from ydb.tests.fq.s3.sql_client import S3Client
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config

MOTO_SERVER_PATH = 'contrib/python/moto/bin/moto_server'
S3_PID_FILE = 's3.pid'


@pytest.fixture(scope='module')
def s3():
    with library.python.port_manager.PortManager() as port_manager:
        port = port_manager.get_port()
        url = f'http://localhost:{port}'
        command = [yatest.common.binary_path(MOTO_SERVER_PATH), 's3', '--host', '::1', '--port', str(port)]

        def is_ready():
            try:
                requests.get(url, timeout=1).raise_for_status()
                return True
            except requests.RequestException as error:
                logging.debug(error)
                return False

        recipes_common.start_daemon(
            command=command, environment=None, is_alive_check=is_ready, pid_file_name=S3_PID_FILE
        )
        try:
            yield S3(url)
        finally:
            with open(S3_PID_FILE) as pid_file:
                recipes_common.stop_daemon(int(pid_file.read()))


@pytest.fixture(scope='module')
def kikimr_settings(request):
    return getattr(request, 'param', {})


@pytest.fixture(scope='module')
def kikimr_params(request):
    return request


@pytest.fixture(scope='module')
def kikimr(kikimr_params, kikimr_settings, s3):
    config = get_ydb_config(kikimr_params)
    config.yaml_config['log_config']['default_level'] = 7
    flags = config.yaml_config['feature_flags']
    flags['enable_external_source_schema_inference'] = kikimr_settings.get('enable_schema_inference', True)
    flags['enable_replace_if_exists_for_external_entities'] = kikimr_settings.get('is_replace_if_exists', False)
    flags['disable_old_secret_creation'] = False
    config.yaml_config['table_service_config']['query_limits']['result_rows_limit'] = 100000
    gateway = config.yaml_config['query_service_config'].setdefault('s3', {})
    gateway['generator_paths_limit'] = 50000
    params = getattr(kikimr_params, 'param', {})
    for name, value in params.items():
        if name in ('inflight', 'data_inflight', 'allow_concurrent_listings'):
            gateway[{'inflight': 'max_inflight'}.get(name, name)] = value
        elif name == '':
            gateway['file_size_limit'] = value
        elif name != 'compute':
            gateway.setdefault('format_size_limit', []).append({'name': name, 'file_size_limit': value})
    if 'inflight' in params:
        http_gateway = config.yaml_config['query_service_config'].setdefault('http_gateway', {})
        http_gateway['max_in_flight_count'] = params['inflight']
    cluster = Kikimr(config)
    try:
        if params.get('compute', 2) > 2:
            cluster.cluster.register_and_start_slots(database=cluster.get_database_name(), count=params['compute'] - 2)
        cluster.wait_kqp_node_count(params.get('compute', 2))
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def unique_prefix():
    return 's3_' + uuid.uuid4().hex + '_'


@pytest.fixture
def client(kikimr, s3, unique_prefix, monkeypatch):
    # The SDK has no PG result decoder; PG values use text_value on the wire.
    # Scope this compatibility adapter to the test, preserving PG column types.
    if 'pg_type' not in convert._to_native_map:
        monkeypatch.setitem(
            convert._to_native_map,
            'pg_type',
            lambda type_pb, value_pb, settings: value_pb.text_value,
        )
    directory = kikimr.get_database_name() + '/' + unique_prefix.rstrip('_')
    kikimr.ydb_client.driver.scheme_client.make_directory(directory)
    result = S3Client(kikimr.ydb_client, s3.s3_url, directory)
    try:
        yield result
    finally:
        result.close()
