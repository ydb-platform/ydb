import os
import pytest
import random
import string

from ydb.tests.fq.streaming.common import Kikimr
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


@pytest.fixture(scope="module")
def kikimr(request):
    param = getattr(request, "param", {})
    enable_watermarks = param.get("enable_watermarks", False)
    enable_shared_reading_in_streaming_queries = param.get("enable_shared_reading_in_streaming_queries", True)

    def get_ydb_config():
        extra_feature_flags = {
            "enable_external_data_sources",
            "enable_streaming_queries",
            "enable_streaming_queries_counters",
            "enable_topics_sql_io_operations",
            "enable_streaming_queries_pq_sink_deduplication"
        }
        if enable_shared_reading_in_streaming_queries:
            extra_feature_flags.add("enable_shared_reading_in_streaming_queries")

        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            pq_client_service_types=["yandex-query"],
            extra_feature_flags=extra_feature_flags,
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

    checkpointing_period_ms = param.get("checkpointing_period_ms", "200")
    os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = checkpointing_period_ms
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
