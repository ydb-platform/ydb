import pytest

from ydb.tests.library.harness import tls_tools
from ydb.tests.library.harness.kikimr_config import GRPC_TLS_DATA_FILES, KikimrConfigGenerator

from yql.essentials.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig


TLS_DATA = {
    'ca.pem': b'test-ca',
    'cert.pem': b'test-certificate',
    'key.pem': b'test-private-key',
}


def _write_tls_data(tls_data_path):
    for filename, data in TLS_DATA.items():
        (tls_data_path / filename).write_bytes(data)


def _tls_data_snapshot(tls_data_path):
    return {
        filename: (
            (tls_data_path / filename).read_bytes(),
            (tls_data_path / filename).stat().st_ino,
            (tls_data_path / filename).stat().st_mtime_ns,
        )
        for filename in GRPC_TLS_DATA_FILES
    }


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
    nbs_database_name = "/Root/NBS"
    cfg_gen = KikimrConfigGenerator(
        enable_nbs=True,
        nbs_database_name=nbs_database_name
    )
    yaml_config = cfg_gen.yaml_config

    # Check that NBS config is present and enabled
    assert "nbs_config" in yaml_config
    assert yaml_config["nbs_config"]["enabled"] is True

    # Check NBS storage config
    nbs_storage_config = yaml_config["nbs_config"]["nbs_storage_config"]
    assert nbs_storage_config["scheme_shard_dir"] == nbs_database_name
    assert nbs_storage_config["folder_id"] == "testFolder"
    assert nbs_storage_config["ssd_system_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["ssd_log_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["ssd_index_channel_pool_kind"] == "hdd"
    assert nbs_storage_config["pipe_client_retry_count"] == 3
    assert nbs_storage_config["pipe_client_min_retry_time"] == 1
    assert nbs_storage_config["pipe_client_max_retry_time"] == 10


def test_kikimr_config_generator_nbs_config_default_database():
    # Test with default nbs_database_name value
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


def test_generated_grpc_tls_data_is_materialized_on_construction(tmp_path, monkeypatch):
    tls_data_path = tmp_path / 'tls'
    tls_data_path.mkdir()
    generated_cert = b'generated-certificate'
    generated_key = b'generated-private-key'

    monkeypatch.setattr(
        tls_tools,
        'generate_selfsigned_cert',
        lambda hostname: (generated_cert, generated_key),
    )

    cfg_gen = KikimrConfigGenerator(
        grpc_ssl_enable=True,
        grpc_tls_data_path=str(tls_data_path),
    )

    assert cfg_gen.grpc_tls_ca == generated_cert
    assert cfg_gen.grpc_tls_cert == generated_cert
    assert cfg_gen.grpc_tls_key == generated_key
    assert (tls_data_path / 'ca.pem').read_bytes() == generated_cert
    assert (tls_data_path / 'cert.pem').read_bytes() == generated_cert
    assert (tls_data_path / 'key.pem').read_bytes() == generated_key


def test_existing_grpc_tls_data_is_loaded_without_rewrite(tmp_path):
    tls_data_path = tmp_path / 'tls'
    tls_data_path.mkdir()
    _write_tls_data(tls_data_path)
    original_snapshot = _tls_data_snapshot(tls_data_path)

    cfg_gen = KikimrConfigGenerator(
        grpc_ssl_enable=True,
        grpc_tls_data_path=str(tls_data_path),
        generate_grpc_tls_data=False,
    )

    assert cfg_gen.grpc_tls_ca == TLS_DATA['ca.pem']
    assert cfg_gen.grpc_tls_cert == TLS_DATA['cert.pem']
    assert cfg_gen.grpc_tls_key == TLS_DATA['key.pem']
    assert cfg_gen.yaml_config['grpc_config']['ca'] == str(tls_data_path / 'ca.pem')
    assert cfg_gen.yaml_config['grpc_config']['cert'] == str(tls_data_path / 'cert.pem')
    assert cfg_gen.yaml_config['grpc_config']['key'] == str(tls_data_path / 'key.pem')

    configs_path = tmp_path / 'configs'
    configs_path.mkdir()
    cfg_gen.write_proto_configs(str(configs_path))

    assert _tls_data_snapshot(tls_data_path) == original_snapshot


def test_existing_grpc_tls_data_requires_path():
    with pytest.raises(ValueError, match='grpc_tls_data_path'):
        KikimrConfigGenerator(
            grpc_ssl_enable=True,
            generate_grpc_tls_data=False,
        )


@pytest.mark.parametrize('invalid_filename', GRPC_TLS_DATA_FILES)
@pytest.mark.parametrize('invalid_kind', ('missing', 'empty', 'directory'))
def test_existing_grpc_tls_data_rejects_invalid_files(tmp_path, invalid_filename, invalid_kind):
    tls_data_path = tmp_path / 'tls'
    tls_data_path.mkdir()

    for filename, data in TLS_DATA.items():
        if filename != invalid_filename:
            (tls_data_path / filename).write_bytes(data)

    invalid_path = tls_data_path / invalid_filename
    if invalid_kind == 'empty':
        invalid_path.touch()
    elif invalid_kind == 'directory':
        invalid_path.mkdir()

    with pytest.raises(ValueError) as error:
        KikimrConfigGenerator(
            grpc_ssl_enable=True,
            grpc_tls_data_path=str(tls_data_path),
            generate_grpc_tls_data=False,
        )

    assert invalid_filename in str(error.value)


def test_disabled_grpc_tls_does_not_validate_data_path(tmp_path):
    tls_data_path = tmp_path / 'does-not-exist'

    cfg_gen = KikimrConfigGenerator(
        grpc_ssl_enable=False,
        grpc_tls_data_path=str(tls_data_path),
        generate_grpc_tls_data=False,
    )

    assert cfg_gen.grpc_tls_data_path is None
    assert not tls_data_path.exists()
