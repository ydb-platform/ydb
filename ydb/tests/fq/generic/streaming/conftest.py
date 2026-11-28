from typing import Final

import pytest

from ydb.tests.fq.generic.utils.settings import Settings
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env

docker_compose_file_path: Final = "ydb/tests/fq/generic/streaming/docker-compose.yml"


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_file_path=docker_compose_file_path)


@pytest.fixture
def kikimr(request, settings):
    set_test_env(request)
    config = get_ydb_config(request, enable_fq_connector=settings)
    config.yaml_config["feature_flags"]["enable_schema_secrets"] = True
    cluster = Kikimr(config)
    try:
        cluster.wait_kqp_node_count(2)
        yield cluster
    finally:
        cluster.stop()
