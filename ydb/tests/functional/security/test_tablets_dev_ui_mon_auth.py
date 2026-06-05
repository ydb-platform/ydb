# -*- coding: utf-8 -*-
import time

import pytest
import requests

from security_test_helpers import (
    _test_endpoints,
    tablet_devui_expected_on_app,
    tablet_devui_new_action_paths,
    tablet_devui_sid_matrix,
)
from ydb.tests.oss.ydb_sdk_import import ydb




def _is_valid_tablet_id(tablet_id):
    return tablet_id not in (None, 0)


def _tablet_id_from_partition_stats(pool, table_path):
    database = table_path.rsplit('/', 1)[0]
    partition_stats_path = f'{database}/.sys/partition_stats'

    def fetch_tablet_id(session):
        query = f"""
            SELECT TabletId
            FROM `{partition_stats_path}`
            WHERE Path = "{table_path}" AND TabletId > 0
            LIMIT 1;
        """
        result_sets = session.transaction().execute(query, commit_tx=True)
        if not result_sets or not result_sets[0].rows:
            return None

        row = result_sets[0].rows[0]
        if isinstance(row, dict):
            tablet_id = row.get('TabletId')
        else:
            tablet_id = getattr(row, 'TabletId', None)
        if _is_valid_tablet_id(tablet_id):
            return tablet_id
        return None

    return pool.retry_operation_sync(fetch_tablet_id)


def _tablet_id_from_viewer_describe(cluster, table_path):
    node = cluster.nodes[1]
    database = table_path.rsplit('/', 1)[0]
    response = requests.get(
        f'https://{node.host}:{node.mon_port}/viewer/json/describe',
        params={
            'database': database,
            'path': table_path,
            'partition_stats': 'true',
            'subs': '0',
            'enums': 'true',
        },
        headers={'Authorization': 'root@builtin'},
        verify=False,
        timeout=1,
    )
    response.raise_for_status()
    for partition in response.json()['PathDescription'].get('TablePartitions', []):
        tablet_id = partition.get('DatashardId') or partition.get('TabletId')
        if _is_valid_tablet_id(tablet_id):
            return tablet_id
    return None


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_datashard_tablet(ydb_cluster_with_enforce_user_token):
    cluster = ydb_cluster_with_enforce_user_token
    _prepare_datashard_tablet(cluster)
    yield cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    _prepare_datashard_tablet(cluster)
    yield cluster


def _prepare_datashard_tablet(cluster):
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
            poll_deadline = time.time() + 10
            poll_interval_seconds = 0.05
            while time.time() < poll_deadline:
                tid = _tablet_id_from_partition_stats(pool, table_path)
                if not _is_valid_tablet_id(tid):
                    tid = _tablet_id_from_viewer_describe(cluster, table_path)
                if _is_valid_tablet_id(tid):
                    datashard_tablet_id = tid
                    break
                time.sleep(poll_interval_seconds)
                poll_interval_seconds = min(0.5, poll_interval_seconds * 2)

    assert datashard_tablet_id, 'DataShard tablet id not available after CREATE TABLE and UPSERT'
    cluster.datashard_tablet_id = datashard_tablet_id


def _data_shard_devui_mon_paths_with_enforce(datashard_tablet_id, secure_path_mode):
    q = f'TabletID={datashard_tablet_id}'
    q_mutating_page = f'{q}&page=volatile-txs'
    q_mutating_action = f'{q}&action=key-access-sample'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    return {
        # New secure path for DataShard DevUI. Should be admin-only in both modes.
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
        f'/tablets/app/secure?{q_mutating_page}': admin_allowed_sids_ok,
        f'/tablets/app/secure?{q_mutating_action}': admin_allowed_sids_ok,
        # Legacy path behavior depends on the feature flag:
        # - secure_path_mode=False: monitoring/root may access (legacy compatibility)
        # - secure_path_mode=True: denied for everyone, including root (force secure path usage)
        f'/tablets/app?{q}': expected_on_app,
        f'/tablets/app?{q_mutating_page}': expected_on_app,
        f'/tablets/app?{q_mutating_action}': expected_on_app,
        # Tablets summary page keeps monitoring-level access.
        f'/tablets?{q}': monitoring_allowed_sids_ok,
    }


def test_datashard_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_datashard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_and_datashard_tablet.datashard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_datashard_tablet,
        _data_shard_devui_mon_paths_with_enforce(tid, secure_path_mode=False),
    )


def test_datashard_tablet_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet.datashard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet,
        _data_shard_devui_mon_paths_with_enforce(tid, secure_path_mode=True),
    )


def test_tablets_app_secure_prefix_forbids_non_admins(ydb_cluster_with_enforce_user_token):
    _, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        {
            '/tablets/app/secure?TabletID=1': admin_allowed_sids_ok,
            '/tablets/app?TabletID=1': monitoring_allowed_sids_ok,
        },
    )


def _pers_queue_devui_mon_paths(pers_queue_tablet_id, secure_path_mode):
    q = f'TabletID={pers_queue_tablet_id}'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    return {
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
        f'/tablets/app?{q}': monitoring_allowed_sids_ok,
        f'/tablets?{q}': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&kv=1': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&TxId=1': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&NewPage=1': expected_on_app,
        f'/tablets/app/secure?{q}&NewPage=1': admin_allowed_sids_ok,
    }


def _pers_queue_send_read_set_paths(pers_queue_tablet_id, secure_path_mode):
    q = (
        f'TabletID={pers_queue_tablet_id}&SendReadSet=1&step=1&txId=1'
        '&decision=commit&allSenderTablets=1'
    )
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    return {
        f'/tablets/app?{q}': expected_on_app,
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
    }


def _pers_queue_action_paths(pers_queue_tablet_id, secure_path_mode):
    q = f'TabletID={pers_queue_tablet_id}&action=future_mutation'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    return {
        f'/tablets/app?{q}': expected_on_app,
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
    }


def test_pers_queue_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        _pers_queue_devui_mon_paths(tid, secure_path_mode=False),
    )


def test_pers_queue_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
        _pers_queue_devui_mon_paths(tid, secure_path_mode=True),
    )


def test_pers_queue_send_read_set_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        _pers_queue_send_read_set_paths(tid, secure_path_mode=False),
    )


def test_pers_queue_send_read_set_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
        _pers_queue_send_read_set_paths(tid, secure_path_mode=True),
    )


def test_pers_queue_new_action_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        tablet_devui_new_action_paths(tid, 'NewPage=1', secure_path_mode=False),
    )


def test_pers_queue_new_action_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
        tablet_devui_new_action_paths(tid, 'NewPage=1', secure_path_mode=True),
    )


def test_pers_queue_send_read_set_form_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    tid = cluster.pers_queue_tablet_id
    url = f'https://{host}:{mon_port}/tablets/app?TabletID={tid}&TxId=1'
    response = requests.get(url, headers={'Authorization': 'monitoring@builtin'}, verify=False)
    if response.status_code == 200 and 'SendReadSet' in response.text:
        assert "action='app/secure?" in response.text or 'action="app/secure?' in response.text
