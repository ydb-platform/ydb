# -*- coding: utf-8 -*-
import pytest
import itertools

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestBatchOperations(RollingUpgradeAndDowngradeFixture):
    table_name = "batch_table"
    pk_1 = list(range(0, 10))
    pk_2 = ["NULL"] + list(range(0, 10))
    groups_cnt = 3

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(table_service_config={
            "enable_oltp_sink": True,
            "enable_batch_updates": True,
        })

    def test_batch_update(self):
        self._fill_table()

        value = 0
        for _ in self.roll():
            self._assert_batch(
                self._q_batch_update(set=[f"v1 = {value}", f"v2 = \"Value{value}\""], where_and=[], where_or=[]),
                self._q_select(where_and=[], where_or=[f"v1 != {value}", f"v2 != \"Value{value}\""])
            )

            value += 1

            self._assert_batch(
                self._q_batch_update(set=[f"v1 = {value}", f"v2 = \"Value{value}\""], where_and=[f"id = {value % self.groups_cnt}"], where_or=[]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt}"], where_or=[f"v1 != {value}", f"v2 != \"Value{value}\""])
            )

            value += 1

            self._assert_batch(
                self._q_batch_update(set=[f"v1 = {value}", f"v2 = \"Value{value}\""], where_and=[], where_or=[f"id = {value % self.groups_cnt}", "k1 % 2 = 0"]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt} OR k1 % 2 = 0"], where_or=[f"v1 != {value}", f"v2 != \"Value{value}\""])
            )

            value += 1

            self._assert_batch(
                self._q_batch_update(set=[f"v1 = {value}", f"v2 = \"Value{value}\""], where_and=[f"id = {value % self.groups_cnt}", f"k2 IS NOT NULL AND k2 <= {value % 5}"], where_or=[]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt}", f"k2 IS NOT NULL AND k2 <= {value % 5}"], where_or=[f"v1 != {value}", f"v2 != \"Value{value}\""])
            )

            value += 1

    def test_batch_delete(self):
        self._fill_table()

        value = 0
        for _ in self.roll():
            self._assert_batch(
                self._q_batch_delete(where_and=[f"id = {value % self.groups_cnt}"], where_or=[]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt}"], where_or=[])
            )

            value += 1

            self._assert_batch(
                self._q_batch_delete(where_and=[f"id = {value % self.groups_cnt}", f"k2 IS NULL OR k2 >= {value % 5}"], where_or=[]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt}", f"k2 IS NULL OR k2 >= {value % 5}"], where_or=[])
            )

            value += 1

            self._assert_batch(
                self._q_batch_delete(where_and=[], where_or=[f"id = {value % self.groups_cnt}", "k1 % 2 = 0"]),
                self._q_select(where_and=[f"id = {value % self.groups_cnt} OR k1 % 2 = 0"], where_or=[])
            )

            value += 1

            self._assert_batch(
                self._q_batch_delete(where_and=[]),
                self._q_select(where_and=[], where_or=[])
            )

            value += 1
            self._fill_table(False)


    # Create and fill the table
    def _fill_table(self, create=True):
        rows = []
        id_value = 0

        for k1, k2 in itertools.product(self.pk_1, self.pk_2):
            v1 = k1 + k2 if k2 != "NULL" else k1
            v2 = f"\"Value{str(k1) + str(k2)}\""
            rows.append([k1, k2, v1, v2, id_value % self.groups_cnt])
            id_value += 1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            if create:
                session_pool.execute_with_retries(self._q_create())
            session_pool.execute_with_retries(self._q_upsert(rows))


    # Execute BATCH query and check new updates
    def _assert_batch(self, batch_query, select_query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(batch_query)
            result_sets = session_pool.execute_with_retries(select_query)
            assert result_sets[0].rows[0]["cnt"] == 0


    # Queries
    def _q_create(self):
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

    def _q_upsert(self, rows):
        return f"""
            UPSERT INTO `{self.table_name}` (k1, k2, v1, v2, id)
            VALUES {", ".join(["(" + ", ".join(map(str, row)) + ")" for row in rows])};
        """

    def _q_batch_update(self, set, where_and, where_or):
        where_list = []
        if len(where_and) > 0:
            where_list.append(" AND ".join(where_and))
        if len(where_or) > 0:
            where_list.append("(" + " OR ".join(where_or) + ")")
        where = " AND ".join(where_list)

        return f"""
            BATCH UPDATE `{self.table_name}`
            SET {", ".join(set)}
            {("WHERE " + where) if len(where) > 0 else ""};
        """

    def _q_batch_delete(self, where_and, where_or):
        where_list = []
        if len(where_and) > 0:
            where_list.append(" AND ".join(where_and))
        if len(where_or) > 0:
            where_list.append("(" + " OR ".join(where_or) + ")")
        where = " AND ".join(where_list)

        return f"""
            BATCH DELETE FROM `{self.table_name}`
            {("WHERE " + where) if len(where) > 0 else ""};
        """

    def _q_select(self, where_and, where_or):
        where_list = []
        if len(where_and) > 0:
            where_list.append(" AND ".join(where_and))
        if len(where_or) > 0:
            where_list.append("(" + " OR ".join(where_or) + ")")
        where = " AND ".join(where_list)

        return f"""
            SELECT COUNT(*) AS cnt FROM `{self.table_name}`
            {("WHERE " + where) if len(where) > 0 else ""};
        """
