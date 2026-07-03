import os
import tempfile
import unittest

from ydb.public.tools.lib.cmds import (
    generic_connector_config,
    resolve_deploy_config_action,
    same_config_path,
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
