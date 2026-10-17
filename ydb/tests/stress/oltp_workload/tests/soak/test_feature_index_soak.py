# -*- coding: utf-8 -*-
import logging
import os

import pytest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
from ydb.tests.stress.oltp_workload.workload import WorkloadRunner


logger = logging.getLogger("FeatureIndexSoak")


def _parse_seeds(value):
    seeds = [int(item.strip(), 0) for item in value.split(",") if item.strip()]
    if not seeds:
        raise ValueError("YDB_FEATURE_INDEX_SOAK_SEEDS must contain at least one integer seed")
    return seeds


class TestFeatureIndexSoak(StressFixture):
    @staticmethod
    def _feature_flags():
        return {
            "enable_vector_index": True,
            "enable_fulltext_index": True,
            "enable_fulltext_index_prefix": True,
            "enable_fulltext_index_row_id": True,
            "enable_compact_fulltext_index": True,
            "enable_add_unique_index": True,
            "enable_json_index": True,
            "enable_json_index_auto_select": True,
        }

    @pytest.fixture(scope="function")
    def setup_soak(self):
        yield from self.setup_cluster(
            extra_feature_flags=self._feature_flags(),
            table_service_config={
                "enable_hybrid_search": True,
                "enable_index_stream_write": True,
            },
        )

    def test_feature_index_soak(self, setup_soak):
        total_duration = int(os.getenv("YDB_FEATURE_INDEX_SOAK_DURATION", "1800"), 0)
        seeds_value = os.getenv(
            "YDB_FEATURE_INDEX_SOAK_SEEDS",
            "0x13579bdf,0x2468ace0,0x5eed2026",
        )
        seeds = _parse_seeds(seeds_value)
        if total_duration < len(seeds):
            raise ValueError("soak duration must allow at least one second per seed")

        logger.info(
            "feature-index soak duration=%ds seeds=%s replay: "
            "YDB_FEATURE_INDEX_SOAK_DURATION=%d YDB_FEATURE_INDEX_SOAK_SEEDS=%s",
            total_duration, seeds, total_duration, seeds_value,
        )

        durations = [total_duration // len(seeds)] * len(seeds)
        durations[-1] += total_duration % len(seeds)

        client = InstrumentedYdbClient(self.endpoint, self.database, True)
        client.wait_connection()
        try:
            for run, (seed, duration) in enumerate(zip(seeds, durations)):
                path = f"feature_index_soak_seed_{seed:08x}"
                logger.info(
                    "feature-index soak run=%d/%d seed=%d duration=%ds path=%s",
                    run + 1, len(seeds), seed, duration, path,
                )
                with WorkloadRunner(client, path, duration, seed=seed) as runner:
                    runner.run(enabled_workloads={
                        "combined_indexes",
                        "fulltext_index",
                        "json_index",
                    })
        finally:
            client.close()
