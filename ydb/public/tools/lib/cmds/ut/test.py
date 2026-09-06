import os
import tempfile
import unittest
from unittest import mock

import pytest

from ydb.public.tools.lib import cmds

from ydb.public.tools.lib.cmds import (
    EmptyArguments,
    generic_connector_config,
    parse_grpc_tls_enable,
    produce_arguments,
    resolve_deploy_config_action,
    resolve_http_proxy_config,
    same_config_path,
    should_generate_grpc_tls_data,
    should_preserve_existing_config,
)
from yql.essentials.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


@pytest.mark.parametrize('tiny_mode', ['true', 'false'])
@pytest.mark.parametrize('actor_system_config', [None, {'use_auto_config': True, 'cpu_count': 1}])
def test_deploy_actor_system_override(tmp_path, monkeypatch, tiny_mode, actor_system_config):
    monkeypatch.setenv('YDB_TINY_MODE', tiny_mode)
    monkeypatch.setenv('YDB_GRPC_ENABLE_TLS', 'false')
    arguments = EmptyArguments()
    arguments.ydb_working_dir = str(tmp_path)
    arguments.ydb_binary_path = '/ydbd'

    class ConfigurationCaptured(Exception):
        pass

    # Exercise the real generator, stopping before any server process is started.
    with mock.patch.object(cmds, 'KiKiMR', side_effect=ConfigurationCaptured) as cluster:
        with pytest.raises(ConfigurationCaptured):
            if actor_system_config is None:
                cmds.deploy(arguments)
            else:
                cmds.deploy(arguments, actor_system_config=actor_system_config)

    configuration = cluster.call_args.args[0]
    actual = configuration.yaml_config['actor_system_config']
    if actor_system_config is None:
        assert not actual.get('use_auto_config', False)
        assert [(pool['name'], pool['threads']) for pool in actual['executor']] == [
            ('System', 2), ('User', 3), ('Batch', 2), ('IO', 1), ('IC', 1),
        ]
    else:
        assert actual == actor_system_config
    assert configuration.tiny_mode == (tiny_mode == 'true')


@pytest.mark.parametrize('external', [True, False])
def test_deploy_actor_override_preserves_custom_yaml(tmp_path, monkeypatch, external):
    monkeypatch.setenv('YDB_GRPC_ENABLE_TLS', 'false')
    arguments = EmptyArguments()
    arguments.ydb_working_dir = str(tmp_path)
    arguments.ydb_binary_path = '/ydbd'
    configs = tmp_path / 'configs'
    configs.mkdir()
    target = configs / 'config.yaml'
    original = 'actor_system_config:\n  use_auto_config: true\n  cpu_count: 3\n'
    if external:
        source = tmp_path / 'custom.yaml'
        source.write_text(original)
        arguments.config_path = str(source)
    else:
        target.write_text(original)

    class ConfigurationCaptured(Exception):
        pass

    with mock.patch.object(cmds, 'KiKiMR', side_effect=ConfigurationCaptured) as cluster:
        with pytest.raises(ConfigurationCaptured):
            cmds.deploy(arguments, actor_system_config={'use_auto_config': True, 'cpu_count': 1})
    cluster.call_args.args[0].write_proto_configs(str(configs))
    assert target.read_text() == original


def test_kikimr_config_generator_generic_connector_config():
    os.environ["FQ_CONNECTOR_ENDPOINT"] = "grpc://localhost:50051"

    expected = TGenericConnectorConfig()
    expected.Endpoint.host = "localhost"
    expected.Endpoint.port = 50051
    expected.UseSsl = False

    actual = generic_connector_config()
    assert actual == expected

    os.environ["FQ_CONNECTOR_ENDPOINT"] = "grpcs://localhost:50051"

    expected = TGenericConnectorConfig()
    expected.Endpoint.host = "localhost"
    expected.Endpoint.port = 50051
    expected.UseSsl = True

    actual = generic_connector_config()
    assert actual == expected


def test_should_preserve_existing_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, 'config.yaml')
        assert should_preserve_existing_config(target) is False

        with open(target, 'w') as writer:
            writer.write('custom: true\n')
        assert should_preserve_existing_config(target) is True

        with open(target, 'w') as writer:
            writer.write('')
        assert should_preserve_existing_config(target) is False

        config_as_dir = os.path.join(tmpdir, 'config-as-dir')
        os.mkdir(config_as_dir)
        assert should_preserve_existing_config(config_as_dir) is False


def test_resolve_deploy_config_action():
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, 'config.yaml')
        external = os.path.join(tmpdir, 'external.yaml')

        assert resolve_deploy_config_action(None, target) == 'generate'

        with open(target, 'w') as writer:
            writer.write('custom: true\n')
        assert resolve_deploy_config_action(None, target) == 'preserve'
        assert resolve_deploy_config_action(target, target) == 'preserve'

        with open(external, 'w') as writer:
            writer.write('external: true\n')
        assert resolve_deploy_config_action(external, target) == 'copy'

        with open(target, 'w') as writer:
            writer.write('')
        assert resolve_deploy_config_action(target, target) == 'generate'


def test_same_config_path_resolves_symlinks():
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, 'config.yaml')
        link = os.path.join(tmpdir, 'config-link.yaml')
        with open(target, 'w') as writer:
            writer.write('custom: true\n')
        try:
            os.symlink(target, link)
        except OSError:
            raise unittest.SkipTest('symlinks not supported on this filesystem')
        assert same_config_path(link, target) is True
        assert resolve_deploy_config_action(link, target) == 'preserve'


def test_parse_grpc_tls_enable_accepts_documented_values():
    for value in ('1', 'true', ' TRUE '):
        assert parse_grpc_tls_enable(value) is True

    for value in (None, '0', 'false', 'yes', ''):
        assert parse_grpc_tls_enable(value) is False


def test_should_generate_grpc_tls_data_uses_explicit_path():
    assert should_generate_grpc_tls_data(None) is True

    with tempfile.TemporaryDirectory() as tmpdir:
        assert should_generate_grpc_tls_data(tmpdir) is True

        with open(os.path.join(tmpdir, 'unrelated.pem'), 'w'):
            pass
        assert should_generate_grpc_tls_data(tmpdir) is True

        for filename in ('ca.pem', 'cert.pem', 'key.pem'):
            path = os.path.join(tmpdir, filename)
            with open(path, 'w'):
                pass
            assert should_generate_grpc_tls_data(tmpdir) is False
            os.unlink(path)


def test_resolve_http_proxy_config_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv('YDB_ENABLE_HTTP_PROXY', raising=False)
    monkeypatch.delenv('YDB_ENABLE_SQS_TOPIC_API', raising=False)

    assert resolve_http_proxy_config(EmptyArguments()) is None


def test_resolve_http_proxy_config_enables_datastreams_proxy(monkeypatch):
    monkeypatch.setenv('YDB_ENABLE_HTTP_PROXY', 'true')
    monkeypatch.delenv('YDB_ENABLE_SQS_TOPIC_API', raising=False)

    assert resolve_http_proxy_config(EmptyArguments()) == {
        'enabled': True,
        'yandex_cloud_service_region': ['ru-central1', 'ru-central-1'],
    }


def test_resolve_http_proxy_config_enables_topic_sqs_and_proxy(monkeypatch):
    monkeypatch.delenv('YDB_ENABLE_HTTP_PROXY', raising=False)
    monkeypatch.setenv('YDB_ENABLE_SQS_TOPIC_API', 'true')

    assert resolve_http_proxy_config(EmptyArguments()) == {
        'enabled': True,
        'sqs_topic_enabled': True,
        'ymq_enabled': False,
        'yandex_cloud_service_region': ['ru-central1', 'ru-central-1'],
    }


def test_resolve_http_proxy_config_accepts_command_line_options(monkeypatch):
    monkeypatch.delenv('YDB_ENABLE_HTTP_PROXY', raising=False)
    monkeypatch.delenv('YDB_ENABLE_SQS_TOPIC_API', raising=False)
    arguments = EmptyArguments()
    arguments.enable_sqs_topic_api = True

    assert resolve_http_proxy_config(arguments) == {
        'enabled': True,
        'sqs_topic_enabled': True,
        'ymq_enabled': False,
        'yandex_cloud_service_region': ['ru-central1', 'ru-central-1'],
    }


def test_produce_arguments_accepts_http_proxy_options():
    arguments = produce_arguments(['--enable-http-proxy', '--enable-sqs-topic-api'])

    assert arguments.enable_http_proxy is True
    assert arguments.enable_sqs_topic_api is True


@pytest.fixture
def cgroups(monkeypatch):
    files = {}
    monkeypatch.setattr(cmds, '_read_text', lambda path: files.get(path, ''))
    monkeypatch.setattr(cmds.os, 'sched_getaffinity', lambda pid: set(range(8)), raising=False)
    return files


@pytest.mark.parametrize('quota,expected', [
    ('100000 100000', 1), ('150000 100000', 2), ('50000 100000', 1),
    ('0 100000', 1), ('1600000 100000', 8), ('max 100000', 8), ('', 8), ('bad', 8), ('100000 0', 8),
])
def test_cgroup_v2_quota(cgroups, quota, expected):
    cgroups.update({
        '/proc/self/cgroup': '0::/',
        '/proc/self/mountinfo': '30 20 0:30 / /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/cpu.max': quota,
    })
    assert cmds.available_cpu_count() == expected


@pytest.mark.parametrize('quota,expected', [('250000', 3), ('0', 1), ('-1', 8), ('bad', 8)])
def test_cgroup_v1_quota(cgroups, quota, expected):
    cgroups.update({
        '/proc/self/cgroup': '3:cpu,cpuacct:/docker/container',
        '/proc/self/mountinfo': '30 20 0:30 /docker/container /sys/fs/cgroup/cpu,cpuacct ro - cgroup cgroup rw,cpu,cpuacct',
        '/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us': quota,
        '/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us': '100000',
    })
    assert cmds.available_cpu_count() == expected


def test_parent_limit_and_non_root_membership(cgroups):
    cgroups.update({
        '/proc/self/cgroup': '0::/docker/parent/child',
        '/proc/self/mountinfo': '30 20 0:30 /docker /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/parent/child/cpu.max': '400000 100000',
        '/sys/fs/cgroup/parent/cpu.max': '150000 100000',
        '/sys/fs/cgroup/cpu.max': 'max 100000',
    })
    assert cmds.available_cpu_count() == 2


def test_cpuset_is_tighter_than_quota(cgroups, monkeypatch):
    cgroups.update({
        '/proc/self/cgroup': '0::/',
        '/proc/self/mountinfo': '30 20 0:30 / /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/cpu.max': '400000 100000',
    })
    monkeypatch.setattr(cmds.os, 'sched_getaffinity', lambda pid: {2})
    assert cmds.available_cpu_count() == 1


def test_affinity_without_cgroups(cgroups):
    assert cmds.available_cpu_count() == 8


def test_fallback_without_affinity(cgroups, monkeypatch):
    monkeypatch.delattr(cmds.os, 'sched_getaffinity', raising=False)
    monkeypatch.setattr(cmds.multiprocessing, 'cpu_count', lambda: 3)
    assert cmds.available_cpu_count() == 3


def test_unavailable_cpu_count(cgroups, monkeypatch):
    monkeypatch.delattr(cmds.os, 'sched_getaffinity', raising=False)

    def unavailable():
        raise NotImplementedError

    monkeypatch.setattr(cmds.multiprocessing, 'cpu_count', unavailable)
    assert cmds.available_cpu_count() == 1


def test_read_unavailable_cgroup_file(tmp_path):
    assert cmds._read_text(str(tmp_path / 'missing')) == ''
