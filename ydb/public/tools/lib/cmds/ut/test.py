import os
import tempfile
import unittest

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
