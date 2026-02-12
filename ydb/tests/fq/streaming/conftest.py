import os
import pytest
import random
import string

from ydb.tests.fq.streaming.common import Kikimr
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


@pytest.fixture(scope="module")
def kikimr(request):
    enable_watermarks = getattr(request, "param", {}).get("enable_watermarks", False)

    def get_ydb_config():
        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            pq_client_service_types=["yandex-query"],
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True,
                "enable_streaming_queries_counters": True,
                "enable_topics_sql_io_operations": True,
            },
            query_service_config={
                "available_external_data_sources": ["ObjectStorage", "Ydb", "YdbTopics"],
                "enable_match_recognize": True
            },
            table_service_config={
                "enable_watermarks": enable_watermarks,
                "dq_channel_version": 1,
            },
            default_clusteradmin="root@builtin",
            use_in_memory_pdisks=False,
        )

        config.yaml_config["log_config"]["default_level"] = 8

        return config

    os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = "200"
    os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "5"

    kikimr = Kikimr(get_ydb_config())
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
