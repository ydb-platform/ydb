import os
import tempfile

from ydb.public.tools.lib.cmds import (
    default_kikimr_config_path,
    generic_connector_config,
    should_write_default_config,
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
    actual = generic_connector_config()
    assert actual == expected


def test_default_kikimr_config_path():
    assert default_kikimr_config_path('/ydb_data') == '/ydb_data/cluster/kikimr_configs/config.yaml'
    assert default_kikimr_config_path(None) is None


def test_should_write_default_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, 'config.yaml')
        assert should_write_default_config(None, target) is True

        with open(target, 'w') as writer:
            writer.write('custom: true\n')
        assert should_write_default_config(None, target) is False

        with open(target, 'w') as writer:
            writer.write('')
        assert should_write_default_config(None, target) is True

        external = os.path.join(tmpdir, 'external.yaml')
        with open(external, 'w') as writer:
            writer.write('external: true\n')
        assert should_write_default_config(external, target) is True
        assert should_write_default_config(target, target) is False
