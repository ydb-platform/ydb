# -*- coding: utf-8 -*-
import requests

from security_test_helpers import _test_endpoints

requests.packages.urllib3.disable_warnings()


def _graph_shard_devui_mon_paths_with_enforce(graph_shard_tablet_id):
    q = f'TabletID={graph_shard_tablet_id}'
    mon_ok = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    }
    return {
        f'/tablets/app/secure?{q}': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 403,
            'root@builtin': 200,
        },
        f'/tablets/app?{q}': mon_ok,
        f'/tablets?{q}': mon_ok,
    }


def _graph_shard_admin_actions_with_enforce(graph_shard_tablet_id):
    q = f'TabletID={graph_shard_tablet_id}&action=change_backend&backend=1'
    forbidden_on_app = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 403,
    }
    return {
        f'/tablets/app?{q}': forbidden_on_app,
        f'/tablets/app/secure?{q}': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 403,
            'root@builtin': 200,
        },
    }


def test_graph_shard_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_graph_shard,
        _graph_shard_devui_mon_paths_with_enforce(tid),
    )


def test_graph_shard_admin_actions_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_graph_shard,
        _graph_shard_admin_actions_with_enforce(tid),
    )


def test_graph_shard_change_backend_links_use_secure_path(
    ydb_cluster_with_enforce_user_token_and_graph_shard,
):
    cluster = ydb_cluster_with_enforce_user_token_and_graph_shard
    tid = cluster.graph_shard_tablet_id
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    url = f'https://{host}:{mon_port}/tablets/app?TabletID={tid}'
    response = requests.get(url, headers={'Authorization': 'monitoring@builtin'}, verify=False)
    assert response.status_code == 200, response.text
    assert 'app/secure?' in response.text
    assert 'action=change_backend&backend=0' in response.text
    assert 'action=change_backend&backend=1' in response.text
    assert 'action=change_backend&backend=2' in response.text
    assert f'app?TabletID={tid}&action=change_backend' not in response.text
