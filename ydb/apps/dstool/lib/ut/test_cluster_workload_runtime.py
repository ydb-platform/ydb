from types import SimpleNamespace

import pytest

import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.dstool_cmd_cluster_workload_run as workload_run
import ydb.public.api.protos.ydb_bridge_common_pb2 as ydb_bridge_common
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


def test_disconnect_socket_request_carries_security_token(monkeypatch):
    monkeypatch.setattr(common.connection_params, 'token', 'secret-token')

    request = workload_run._make_disconnect_socket_request(17)

    assert request.ClosePeerSocketNodeId == 17
    assert request.SecurityToken == 'secret-token'


def test_readonly_vdisk_or_pdisk_is_not_counted_as_healthy_for_fail_model():
    vslot = SimpleNamespace(Ready=True)
    vslot_id = (1, 2, 3)

    assert workload_run._vslot_is_writable_and_healthy(
        vslot_id, vslot, True, set(), set()
    )
    assert not workload_run._vslot_is_writable_and_healthy(
        vslot_id, vslot, True, {vslot_id}, set()
    )
    assert not workload_run._vslot_is_writable_and_healthy(
        vslot_id, vslot, True, set(), {(1, 2)}
    )


def test_restart_attempt_is_recorded_before_recovery_wait(monkeypatch):
    attempts = []
    monkeypatch.setattr(workload_run.subprocess, 'check_call', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        workload_run,
        '_wait_for_node_restart',
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('timed out')),
    )

    with pytest.raises(RuntimeError, match='timed out'):
        workload_run._restart_node_process(
            1,
            100,
            'node-one',
            '123',
            '9',
            0,
            lambda: attempts.append('recorded'),
        )

    assert attempts == ['recorded']


def test_pdisk_key_config_is_valid_textproto_without_shell_escaping():
    proto = workload_run.key_proto.TKeyConfig()
    proto.Keys.add(
        ContainerPath='/key/path',
        Pin=b'',
        Id='Key2',
        Version=2,
    )
    config = workload_run.make_pdisk_key_config(proto)

    assert 'ContainerPath: "/key/path"' in config
    assert r'\"' not in config


def test_read_pdisk_key_config_preserves_existing_non_default_keys(monkeypatch):
    existing = b'''\
Keys { ContainerPath: "/secret/old" Pin: "old-pin" Id: "Old" Version: 7 }
'''
    monkeypatch.setattr(
        workload_run.subprocess,
        'run',
        lambda *args, **kwargs: SimpleNamespace(stdout=existing, stderr=b''),
    )

    config = workload_run._read_pdisk_key_config('node-one')

    assert len(config.Keys) == 1
    assert config.Keys[0].ContainerPath == '/secret/old'
    assert config.Keys[0].Pin == b'old-pin'
    assert config.Keys[0].Version == 7


@pytest.mark.parametrize(
    'arguments',
    [
        b'ydbd\0server\0--pdisk-key-file\0/Berkanavt/kikimr/cfg/pdisk_key.txt\0',
        b'ydbd\0server\0--pdisk-key-file=/Berkanavt/kikimr/cfg/pdisk_key.txt\0',
    ],
)
def test_verify_pdisk_key_config_argument_accepts_expected_path(monkeypatch, arguments):
    monkeypatch.setattr(
        workload_run.subprocess,
        'run',
        lambda *args, **kwargs: SimpleNamespace(stdout=arguments, stderr=b''),
    )

    workload_run._verify_pdisk_key_config_argument('node-one', 123)


def test_verify_pdisk_key_config_argument_rejects_other_path(monkeypatch):
    monkeypatch.setattr(
        workload_run.subprocess,
        'run',
        lambda *args, **kwargs: SimpleNamespace(
            stdout=b'ydbd\0--pdisk-key-file\0/etc/ydb/pdisk-key.txt\0',
            stderr=b'',
        ),
    )

    with pytest.raises(RuntimeError, match='ChangePDiskKey requires'):
        workload_run._verify_pdisk_key_config_argument('node-one', 123)


def test_cms_permission_request_is_node_scoped_and_authenticated(monkeypatch):
    captured = {}

    def invoke_grpc(method, request):
        captured['method'] = method
        captured['request'] = request
        return SimpleNamespace(
            Status=SimpleNamespace(Code=common.kikimr_cms.TStatus.ALLOW, Reason='')
        )

    monkeypatch.setattr(common, 'invoke_grpc', invoke_grpc)
    monkeypatch.setattr(common.connection_params, 'token', 'secret-token')

    error = common.cms_permission_request(
        'workload',
        '42',
        'test action',
        1_000_000,
        common.kikimr_cms.MODE_KEEP_AVAILABLE,
        common.kikimr_cms.TAction.RESTART_SERVICES,
        services=('storage',),
    )

    assert error is None
    assert captured['method'] == 'CmsRequest'
    request = captured['request']
    assert request.SecurityToken == 'secret-token'
    assert request.PermissionRequest.Actions[0].Host == '42'
    assert list(request.PermissionRequest.Actions[0].Services) == ['storage']


def test_update_pile_states_uses_names_quorum_auth_and_checks_status(monkeypatch):
    captured = {}

    def invoke_grpc(method, request, **kwargs):
        captured['method'] = method
        captured['request'] = request
        captured['kwargs'] = kwargs
        return SimpleNamespace(operation=SimpleNamespace(status=StatusIds.SUCCESS))

    monkeypatch.setattr(common, 'invoke_grpc', invoke_grpc)
    monkeypatch.setattr(common.connection_params, 'token', 'admin-token')

    common.update_pile_states(
        (ydb_bridge_common.PileState(
            pile_name='pile-a',
            state=ydb_bridge_common.PileState.DISCONNECTED,
        ),),
        quorum_piles=('pile-b',),
        endpoints=('endpoint',),
    )

    assert captured['method'] == 'UpdateClusterState'
    assert captured['request'].updates[0].pile_name == 'pile-a'
    assert list(captured['request'].quorum_piles) == ['pile-b']
    assert captured['kwargs']['metadata'] == (('x-ydb-auth-ticket', 'admin-token'),)
    assert captured['kwargs']['endpoints'] == ('endpoint',)

    monkeypatch.setattr(
        common,
        'invoke_grpc',
        lambda *args, **kwargs: SimpleNamespace(
            operation=SimpleNamespace(status=StatusIds.UNAUTHORIZED)
        ),
    )
    with pytest.raises(common.QueryError, match='UpdateClusterState failed'):
        common.update_pile_states(())


def test_disconnect_primary_pile_promotes_replacement_in_same_request(monkeypatch):
    captured = []
    monkeypatch.setattr(common, 'update_pile_states', lambda updates, **kwargs: captured.extend(updates))

    common.disconnect_pile('pile-a', 'pile-b')

    assert [(item.pile_name, item.state) for item in captured] == [
        ('pile-b', ydb_bridge_common.PileState.PRIMARY),
        ('pile-a', ydb_bridge_common.PileState.DISCONNECTED),
    ]


def test_connect_pile_uses_live_primary_endpoint_for_both_quorums(monkeypatch):
    captured = []

    def update_pile_states(updates, **kwargs):
        captured.append((tuple(updates), kwargs))

    monkeypatch.setattr(common, 'update_pile_states', update_pile_states)

    common.connect_pile(
        'pile-disconnected',
        'pile-primary',
        {
            'pile-primary': ('primary-endpoint',),
            'pile-disconnected': ('target-endpoint',),
        },
    )

    assert len(captured) == 2
    assert [call[1]['quorum_piles'] for call in captured] == [
        ('pile-primary',),
        ('pile-disconnected',),
    ]
    assert [call[1]['endpoints'] for call in captured] == [
        ('primary-endpoint',),
        ('primary-endpoint',),
    ]
    assert all(
        call[0][0].pile_name == 'pile-disconnected'
        and call[0][0].state == ydb_bridge_common.PileState.NOT_SYNCHRONIZED
        for call in captured
    )


def test_fetch_node_endpoints_supports_tls_and_skips_missing_grpc(monkeypatch):
    monkeypatch.setattr(
        common,
        'fetch_json_info',
        lambda entity, nodes: {
            1: {
                'Host': 'node-one',
                'Endpoints': [{'Name': 'grpcs', 'Address': '[::1]:2136'}],
            },
            2: {
                'Host': 'node-two',
                'Endpoints': [{'Name': 'http-mon', 'Address': ':8765'}],
            },
        },
    )

    endpoints = common.fetch_node_to_endpoint_map()

    assert set(endpoints) == {1}
    assert endpoints[1].protocol == 'grpcs'
    assert endpoints[1].host == '::1'
    assert endpoints[1].grpc_port == 2136
    assert endpoints[1].host_with_grpc_port == '[::1]:2136'


def test_fetch_node_endpoints_falls_back_from_wildcard_address(monkeypatch):
    monkeypatch.setattr(
        common,
        'fetch_json_info',
        lambda entity, nodes: {
            1: {
                'Host': 'node-one',
                'Endpoints': [{'Name': 'grpc', 'Address': '0.0.0.0:2135'}],
            },
        },
    )

    endpoints = common.fetch_node_to_endpoint_map()

    assert endpoints[1].host_with_grpc_port == 'node-one:2135'


def test_wait_for_node_storage_requires_two_healthy_polls(monkeypatch):
    pdisk_calls = iter([
        {(1, 2): {'State': 'InitialFormatRead'}},
        {(1, 2): {'State': 'Normal'}},
        {(1, 2): {'State': 'Normal'}},
    ])
    vdisk_calls = iter([
        {(1, 2, 3): {'VDiskState': 'OK', 'Replicated': True}},
        {(1, 2, 3): {'VDiskState': 'OK', 'Replicated': True}},
        {(1, 2, 3): {'VDiskState': 'OK', 'Replicated': True}},
    ])
    calls = []

    def fetch_json_info(entity, nodes):
        calls.append(entity)
        return next(pdisk_calls if entity == 'pdiskinfo' else vdisk_calls)

    monkeypatch.setattr(common, 'fetch_json_info', fetch_json_info)
    monkeypatch.setattr(workload_run.time, 'sleep', lambda seconds: None)

    workload_run._wait_for_node_storage(1, (2,), ((1, 2, 3),), timeout_seconds=5)

    assert calls == ['pdiskinfo', 'vdiskinfo'] * 3


def test_workload_rejects_dry_run_before_connecting():
    with pytest.raises(common.InvalidParameterError, match='Dry-run is not supported'):
        workload_run.do(SimpleNamespace(dry_run=True))


def test_workload_reports_config_file_errors_as_invalid_parameters(tmp_path):
    missing = tmp_path / 'missing.yaml'

    with pytest.raises(common.InvalidParameterError, match='failed to read workload configuration'):
        workload_run.do(SimpleNamespace(dry_run=False, config_file=str(missing)))
