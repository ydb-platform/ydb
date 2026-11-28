from typing import Final

import pytest

from ydb.tests.fq.generic.utils.settings import Settings
from ydb.tests.fq.generic.utils.ydb_helpers import create_external_data_source
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env

docker_compose_file_path: Final = "ydb/tests/fq/generic/analytics/docker-compose.yml"


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_file_path=docker_compose_file_path)


@pytest.fixture
def kikimr(request, settings):
    set_test_env(request)
    config = get_ydb_config(request, enable_fq_connector=settings)
    config.yaml_config["feature_flags"]["enable_schema_secrets"] = True
    config.yaml_config["query_service_config"]["available_external_data_sources"] += [
        "ClickHouse",
        "Greenplum",
        "MySQL",
        "PostgreSQL",
    ]
    cluster = Kikimr(config)
    try:
        cluster.wait_kqp_node_count(2)
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def client(kikimr):
    return kikimr.ydb_client


@pytest.fixture
def create_source(client, settings):
    def create(name, kind):
        source_type, port = {
            "clickhouse": ("ClickHouse", 9000),
            "greenplum": ("Greenplum", 6432),
            "mysql": ("MySQL", 3306),
            "postgresql": ("PostgreSQL", 6432),
            "ydb": ("Ydb", 2136),
        }[kind]
        database = getattr(settings, kind)
        create_external_data_source(
            client,
            name,
            source_type,
            f"tests-fq-generic-analytics-{kind}:{port}",
            database.dbname,
            token="root@builtin" if kind == "ydb" else None,
            login=database.username if kind != "ydb" else None,
            password=database.password if kind != "ydb" else None,
            protocol=database.protocol if kind == "clickhouse" else "NATIVE" if kind == "postgresql" else None,
        )

    return create
