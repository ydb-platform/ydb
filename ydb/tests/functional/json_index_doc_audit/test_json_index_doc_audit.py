# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


JSON_TYPES = ("Json", "JsonDocument")


class TestJsonIndexReadYourWrites:
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags=[
                "enable_json_index",
                "enable_json_index_auto_select",
            ],
        )
        cls.cluster = KiKiMR(config)
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

    def create_table(self, name, json_type):
        self.execute(
            f"""
                CREATE TABLE `{name}` (
                    id Uint64 NOT NULL,
                    payload {json_type},
                    PRIMARY KEY (id),
                    INDEX json_idx GLOBAL USING json ON (payload)
                );
            """
        )

    @pytest.mark.parametrize("json_type", JSON_TYPES)
    def test_insert_then_select_by_json_index_in_same_transaction(self, json_type):
        table = f"ryw_insert_{json_type.lower()}"
        self.create_table(table, json_type)

        result_sets = self.execute(
            f"""
                UPSERT INTO `{table}` (id, payload) VALUES
                    (1, {json_type}(@@{{"state":"inserted"}}@@));

                SELECT id FROM `{table}` VIEW json_idx
                WHERE JSON_VALUE(payload, '$.state' RETURNING Utf8) = "inserted"u;
            """
        )

        assert [row.id for row in result_sets[0].rows] == [1]

    @pytest.mark.parametrize("json_type", JSON_TYPES)
    def test_update_then_select_by_json_index_in_same_transaction(self, json_type):
        table = f"ryw_update_{json_type.lower()}"
        self.create_table(table, json_type)
        self.execute(
            f"""
                UPSERT INTO `{table}` (id, payload) VALUES
                    (1, {json_type}(@@{{"state":"old"}}@@));
            """
        )

        result_sets = self.execute(
            f"""
                UPDATE `{table}`
                SET payload = {json_type}(@@{{"state":"new"}}@@)
                WHERE id = 1;

                SELECT id FROM `{table}` VIEW json_idx
                WHERE JSON_VALUE(payload, '$.state' RETURNING Utf8) = "new"u;
            """
        )

        assert [row.id for row in result_sets[0].rows] == [1]

    @pytest.mark.parametrize("json_type", JSON_TYPES)
    def test_delete_then_select_by_json_index_in_same_transaction(self, json_type):
        table = f"ryw_delete_{json_type.lower()}"
        self.create_table(table, json_type)
        self.execute(
            f"""
                UPSERT INTO `{table}` (id, payload) VALUES
                    (1, {json_type}(@@{{"state":"deleted"}}@@));
            """
        )

        result_sets = self.execute(
            f"""
                DELETE FROM `{table}` WHERE id = 1;

                SELECT id FROM `{table}` VIEW json_idx
                WHERE JSON_VALUE(payload, '$.state' RETURNING Utf8) = "deleted"u;
            """
        )

        assert list(result_sets[0].rows) == []
