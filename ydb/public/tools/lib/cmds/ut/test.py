import os

from ydb.public.tools.lib.cmds import generic_connector_config, merge_two_yaml_configs
from ydb.library.yql.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


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

def test_merge_two_yaml_configs():
    patched_yaml = yatest.common.output_path("patched.yaml")

    with open(yatest.common.source_path("ydb/public/tools/lib/cmds/ut/config.yaml"), 'r') as fh:
        data_1 = yaml.load(fh, Loader=yaml.FullLoader)
    
    with open(yatest.common.source_path("ydb/public/tools/lib/cmds/ut/patch.yaml"), 'r') as fh:
        data_2 = yaml.load(fh, Loader=yaml.FullLoader)

    with open(patched_yaml, "w") as res:
        res.write(yaml.dump(merge_two_yaml_configs(data_1, data_2), default_flow_style=False))

    return yatest.common.canonical_file(patched_yaml, local=True)
