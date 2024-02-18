from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from ydb.library.yql.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


def test_kikimr_config_generator_generic_connector_config():
    generic_connector_config = TGenericConnectorConfig()
    generic_connector_config.Endpoint.host = "localhost"
    generic_connector_config.Endpoint.port = 50051
    generic_connector_config.UseSsl = False

    cfg_gen = KikimrConfigGenerator(generic_connector_config=generic_connector_config)
    yaml_config = cfg_gen.yaml_config

    assert yaml_config["query_service_config"]["generic"]["connector"]["endpoint"]["host"] == generic_connector_config.Endpoint.host
    assert yaml_config["query_service_config"]["generic"]["connector"]["endpoint"]["port"] == generic_connector_config.Endpoint.port
    assert yaml_config["query_service_config"]["generic"]["connector"]["use_ssl"] == generic_connector_config.UseSsl
    assert yaml_config["query_service_config"]["generic"]["default_settings"] == [
        {"name": "DateTimeFormat", "value": "string"},
        {"name": "UsePredicatePushdown", "value": "true"},
    ]
    assert yaml_config["feature_flags"]["enable_external_data_sources"] is True
    assert yaml_config["feature_flags"]["enable_script_execution_operations"] is True
