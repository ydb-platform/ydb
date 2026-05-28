# -*- coding: utf-8 -*-
from security_test_helpers import _test_endpoints


def _pers_queue_devui_mon_paths_with_enforce(pers_queue_tablet_id):
    q = f'TabletID={pers_queue_tablet_id}'
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


def _pers_queue_send_read_set_with_enforce(pers_queue_tablet_id):
    q = (
        f'TabletID={pers_queue_tablet_id}&SendReadSet=1&step=1&txId=1'
        '&decision=commit&allSenderTablets=1'
    )
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


def _pers_queue_action_default_admin_with_enforce(pers_queue_tablet_id):
    q = f'TabletID={pers_queue_tablet_id}&action=future_mutation'
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


def test_pers_queue_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        _pers_queue_devui_mon_paths_with_enforce(tid),
    )


def test_pers_queue_send_read_set_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        _pers_queue_send_read_set_with_enforce(tid),
    )


def test_pers_queue_new_action_is_admin_only_by_default(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    tid = ydb_cluster_with_enforce_user_token_and_pers_queue_topic.pers_queue_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
        _pers_queue_action_default_admin_with_enforce(tid),
    )
