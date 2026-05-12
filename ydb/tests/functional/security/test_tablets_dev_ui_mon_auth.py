# -*- coding: utf-8 -*-
import time

import pytest
import requests
import ydb.core.protos.msgbus_pb2 as msgbus

from ydb.tests.oss.ydb_sdk_import import ydb


def _scheme_describe_with_table_partitions(cluster, path, token):
    request = msgbus.TSchemeDescribe()
    request.Path = path
    request.SecurityToken = token
    request.Options.ReturnPartitioningInfo = True
    return cluster.client.invoke(request, 'SchemeDescribe')


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_datashard_tablet(ydb_cluster_with_enforce_user_token):
    cluster = ydb_cluster_with_enforce_user_token
    database = '/Root/ds_mon_security'
    cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database, token='root@builtin')

    node = cluster.nodes[1]
    driver_config = ydb.DriverConfig(
        endpoint=f'{node.host}:{node.port}',
        database=database,
        credentials=ydb.AuthTokenCredentials('root@builtin'),
    )
    table_path = f'{database}/ds_mon_t'
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=5)

        def create_table(session):
            session.create_table(
                table_path,
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_primary_key('id'),
            )
            # Force the first DataShard activity right after table creation.
            session.transaction().execute(
                f'UPSERT INTO `{table_path}` (id) VALUES (1);',
                commit_tx=True,
            )

        with ydb.SessionPool(driver) as pool:
            pool.retry_operation_sync(create_table)

    datashard_tablet_id = None
    poll_deadline = time.time() + 45
    poll_interval_seconds = 0.1
    while time.time() < poll_deadline:
        described = _scheme_describe_with_table_partitions(cluster, table_path, 'root@builtin')
        partitions = described.PathDescription.TablePartitions
        tid = None
        if partitions:
            p0 = partitions[0]
            tid = getattr(p0, 'DatashardId', None) or getattr(p0, 'datashard_id', None)
        if tid:
            datashard_tablet_id = tid
            break
        time.sleep(poll_interval_seconds)
        poll_interval_seconds = min(1.0, poll_interval_seconds * 2)
    assert datashard_tablet_id, 'DataShard tablet id not available after CREATE TABLE and UPSERT'
    cluster.datashard_tablet_id = datashard_tablet_id

    yield cluster


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else 'null'
    assert (
        response.status_code == expected_status
    ), f'Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}'


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def _data_shard_devui_mon_paths_with_enforce(datashard_tablet_id):
    q = f'TabletID={datashard_tablet_id}'
    forbidden_on_app = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 403,
    }
    mon_ok = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    }
    admin_only_on_secure_app_path = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 200,
    }
    return {
        f'/tablets/app/secure?{q}': admin_only_on_secure_app_path,
        f'/tablets/app?{q}': forbidden_on_app,
        f'/tablets?{q}': mon_ok,
    }


def test_datashard_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_datashard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_and_datashard_tablet.datashard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_datashard_tablet,
        _data_shard_devui_mon_paths_with_enforce(tid),
    )
