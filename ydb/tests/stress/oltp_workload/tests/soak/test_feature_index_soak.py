# -*- coding: utf-8 -*-
import logging
import os

import pytest

from ydb.tests.library.fixtures.json import JsonPredicateGenerator
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.oss.ydb_sdk_import import ydb
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

    @pytest.mark.parametrize("json_type", ["Json", "JsonDocument"])
    def test_json_index_predicates(self, setup_soak, json_type):
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(f"""
                CREATE TABLE docs (
                    pk Uint64 NOT NULL,
                    json {json_type},
                    PRIMARY KEY (pk),
                    INDEX json_idx GLOBAL USING json ON (json)
                );
            """)
            pool.execute_with_retries(f"""
                UPSERT INTO docs (pk, json) VALUES
                    (1, {json_type}('{{"items":["hit"]}}')),
                    (2, {json_type}('{{"common_type":"base","region":"west"}}')),
                    (3, {json_type}('{{"common_type":"extended","tenant":"a"}}')),
                    (4, {json_type}('{{"common_type":"compact"}}'));
            """)

            def select_keys(predicate, view):
                result = pool.execute_with_retries(f"SELECT pk FROM docs {view} WHERE {predicate} ORDER BY pk;")
                return [row["pk"] for row in result[0].rows]

            wildcard = 'JSON_VALUE(`json`, \'strict $.*[0]\' RETURNING Utf8) != ""'
            assert select_keys(wildcard, "") == select_keys(wildcard, "VIEW PRIMARY KEY") == [1]
            for predicate in JsonPredicateGenerator().explicit_index_predicates:
                assert select_keys(predicate, "VIEW json_idx") == select_keys(predicate, "VIEW PRIMARY KEY"), predicate

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
