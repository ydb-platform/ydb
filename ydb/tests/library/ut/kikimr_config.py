from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from yql.essentials.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


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


def test_kikimr_config_generator_nbs_config():
    nbs_database = "/Root/NBS"
    cfg_gen = KikimrConfigGenerator(
        enable_nbs=True,
        nbs_database=nbs_database
    )
    yaml_config = cfg_gen.yaml_config

    # Check that NBS config is present and enabled
    assert "nbs_config" in yaml_config
    assert yaml_config["nbs_config"]["enabled"] is True

    # Check NBS storage config
    nbs_storage_config = yaml_config["nbs_config"]["nbs_storage_config"]
    assert nbs_storage_config["scheme_shard_dir"] == nbs_database
    assert nbs_storage_config["folder_id"] == "testFolder"
    assert nbs_storage_config["ssd_system_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["ssd_log_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["ssd_index_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["pipe_client_retry_count"] == 3
    assert nbs_storage_config["pipe_client_min_retry_time"] == 1
    assert nbs_storage_config["pipe_client_max_retry_time"] == 10


def test_kikimr_config_generator_nbs_config_default_database():
    # Test with default nbs_database value
    cfg_gen = KikimrConfigGenerator(enable_nbs=True)
    yaml_config = cfg_gen.yaml_config

    # Check that NBS config uses default database path
    assert "nbs_config" in yaml_config
    assert yaml_config["nbs_config"]["enabled"] is True
    assert yaml_config["nbs_config"]["nbs_storage_config"]["scheme_shard_dir"] == "/Root/NBS"


def test_kikimr_config_generator_nbs_disabled():
    cfg_gen = KikimrConfigGenerator()
    yaml_config = cfg_gen.yaml_config

    # Check that NBS config is not present when disabled
    assert "nbs_config" not in yaml_config
