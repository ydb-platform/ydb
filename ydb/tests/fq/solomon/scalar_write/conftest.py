import random
import string

import pytest

from ydb.tests.fq.streaming_common.common import Kikimr
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


def get_solomon_ydb_config():
    """A lightweight single-node cluster with the Solomon external data source enabled."""
    config = KikimrConfigGenerator(
        extra_feature_flags=["enable_external_data_sources"],
        query_service_config={
            "available_external_data_sources": ["Solomon"],
            "solomon": {
                "default_settings": [{"name": "_EnableReading", "value": "true"}]
            }
        },
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=True,
    )
    config.yaml_config["log_config"]["default_level"] = 7
    return config


@pytest.fixture(scope="module")
def kikimr():
    kikimr = Kikimr(get_solomon_ydb_config())
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
