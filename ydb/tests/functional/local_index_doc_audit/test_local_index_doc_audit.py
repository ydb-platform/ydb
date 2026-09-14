# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


class TestLocalIndexDocumentationAudit:
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def test_alter_false_positive_probability_on_row_bloom_index(self):
        self.execute(
            """
                CREATE TABLE `row_bloom_alter` (
                    tenant Utf8 NOT NULL,
                    id Uint64 NOT NULL,
                    PRIMARY KEY (tenant, id),
                    INDEX idx_tenant LOCAL USING bloom_filter
                        ON (tenant)
                        WITH (false_positive_probability = 0.01)
                );
            """
        )

        try:
            self.execute(
                """
                    ALTER TABLE `row_bloom_alter`
                    ALTER INDEX idx_tenant SET (
                        false_positive_probability = 0.5
                    );
                """
            )
        except ydb.Error as error:
            pytest.fail(f"Documented ALTER INDEX was rejected: {error}")
