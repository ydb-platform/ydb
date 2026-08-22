import os
import tempfile
import unittest

import pytest

from ydb.public.tools.lib.cmds import (
    enable_tls,
    generic_connector_config,
    load_existing_grpc_tls_data,
    resolve_deploy_config_action,
    same_config_path,
    should_preserve_existing_config,
)
from yql.essentials.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


@pytest.mark.parametrize(
    ('value', 'expected'),
    (
        ('1', True),
        ('true', True),
        ('TRUE', True),
        ('0', False),
        ('false', False),
        ('', False),
    ),
)
def test_enable_tls_accepts_documented_boolean_values(monkeypatch, value, expected):
    monkeypatch.setenv('YDB_GRPC_ENABLE_TLS', value)
    assert enable_tls() is expected


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


def test_load_existing_grpc_tls_data():
    assert load_existing_grpc_tls_data(None) is None

    with tempfile.TemporaryDirectory() as tmpdir:
        assert load_existing_grpc_tls_data(tmpdir) is None

        for filename, data in (
            ('ca.pem', 'ca'),
            ('cert.pem', 'certificate'),
            ('key.pem', 'private-key'),
        ):
            with open(os.path.join(tmpdir, filename), 'w') as writer:
                writer.write(data)

        assert load_existing_grpc_tls_data(tmpdir) == (b'ca', b'certificate', b'private-key')


def test_load_existing_grpc_tls_data_rejects_incomplete_data():
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = os.path.join(tmpdir, 'cert.pem')
        with open(cert_path, 'w') as writer:
            writer.write('certificate')

        with pytest.raises(ValueError, match='ca.pem.*key.pem'):
            load_existing_grpc_tls_data(tmpdir)

        with open(os.path.join(tmpdir, 'ca.pem'), 'w') as writer:
            writer.write('ca')
        with open(os.path.join(tmpdir, 'key.pem'), 'w'):
            pass

        with pytest.raises(ValueError, match='key.pem'):
            load_existing_grpc_tls_data(tmpdir)
