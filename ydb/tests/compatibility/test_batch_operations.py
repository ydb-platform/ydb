# -*- coding: utf-8 -*-
import pytest
import itertools

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestBatchOperations(RollingUpgradeAndDowngradeFixture):
    table_name = "batch_table"
    pk_1 = list(range(0, 15))
    pk_2 = list(range(0, 15))
    groups_cnt = 3

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(table_service_config={
            "enable_oltp_sink": True,
            "enable_batch_updates": True,
        })

    def test_batch_update(self):
        self.fill_table()

        value = 0
        for _ in self.roll():
            self.assert_batch(
                self.q_batch_update(f"v1 = {value}, v2 = \"String_{value}\""),
                self.q_select(f"v1 != {value} OR v2 != \"String_{value}\"")
            )

            value += 1

            where = f"id = {value % self.groups_cnt}"
            self.assert_batch(
                self.q_batch_update(f"v1 = {value}, v2 = \"String_{value}\"", where),
                self.q_select(f"({where}) AND (v1 != {value} OR v2 != \"String_{value}\")")
            )

            value += 1

            where = f"id = {value % self.groups_cnt} OR k1 % 2 = 0"
            self.assert_batch(
                self.q_batch_update(f"v1 = {value}, v2 = \"String_{value}\"", where),
                self.q_select(f"({where}) AND (v1 != {value} OR v2 != \"String_{value}\")")
            )

            value += 1

            where = f"id = {value % self.groups_cnt} AND k2 IS NOT NULL AND k2 <= {value % 5}"
            self.assert_batch(
                self.q_batch_update(f"v1 = {value}, v2 = \"String_{value}\"", where),
                self.q_select(f"({where}) AND (v1 != {value} OR v2 != \"String_{value}\")")
            )

            value += 1

    def test_batch_delete(self):
        self.fill_table()
        value = 0

        for _ in self.roll():
            where = f"id = {value % self.groups_cnt}"
            self.assert_batch(
                self.q_batch_delete(where),
                self.q_select(where)
            )

            value += 1

            where = f"id = {value % self.groups_cnt} AND (k2 IS NULL OR k2 >= {value % 5})"
            self.assert_batch(
                self.q_batch_delete(where),
                self.q_select(where)
            )

            value += 1

            where = f"id = {value % self.groups_cnt} OR k1 % 2 = 0"
            self.assert_batch(
                self.q_batch_delete(where),
                self.q_select(where)
            )

            value += 1

            self.assert_batch(
                self.q_batch_delete(),
                self.q_select()
            )

            value += 1
            self.fill_table(False)

    def fill_table(self, create=True):
        rows = []
        id_value = 0

        for k1, k2 in itertools.product(self.pk_1, self.pk_2):
            v1 = k1 + k2 if k2 != "NULL" else k1
            v2 = f"\"Value{str(k1) + str(k2)}\""
            rows.append([k1, k2, v1, v2, id_value % self.groups_cnt])
            id_value += 1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            if create:
                session_pool.execute_with_retries(self.q_create())
            session_pool.execute_with_retries(self.q_upsert(rows))

    def assert_batch(self, batch_query, select_query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(batch_query)
            result_sets = session_pool.execute_with_retries(select_query)
            assert result_sets[0].rows[0]["cnt"] == 0

    def q_create(self):
        return f"""
            CREATE TABLE `{self.table_name}` (
                k1 Uint64 NOT NULL,
                k2 Uint64,
                v1 Uint64,
                v2 String,
                id Int64,
                PRIMARY KEY (k2, k1)
            ) WITH (
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3,
                PARTITION_AT_KEYS = ((2, 2), (4), (5, 6), (8))
            );
        """

    def q_upsert(self, rows):
        return f"""
            UPSERT INTO `{self.table_name}` (k1, k2, v1, v2, id)
            VALUES {", ".join(["(" + ", ".join(map(str, row)) + ")" for row in rows])};
        """

    def q_batch_update(self, values, where=None):
        return f"""
            BATCH UPDATE `{self.table_name}`
            SET {values}{(" WHERE " + where) if where else ""};
        """

    def q_batch_delete(self, where=None):
        return f"""
            BATCH DELETE FROM `{self.table_name}`{(" WHERE " + where) if where else ""};
        """

    def q_select(self, where=None):
        return f"""
            SELECT COUNT(*) AS cnt FROM `{self.table_name}`{(" WHERE " + where) if where else ""};
        """
