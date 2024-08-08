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
    first_dict = {'data': {'user': 'root', 'password': '1234'}}
    second_dict = {'data': {'password': '12345678'}}

    final_dict = {'data': {'user': 'root', 'password': '12345678'}}
    assert merge_two_yaml_configs(first_dict, second_dict) == final_dict

    first_dict = {'data': {'user': 'root', 'password': '1234'}}
    second_dict = {}

    final_dict = {'data': {'user': 'root', 'password': '1234'}}
    assert merge_two_yaml_configs(first_dict, second_dict) == final_dict
