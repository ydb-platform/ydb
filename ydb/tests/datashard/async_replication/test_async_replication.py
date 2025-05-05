import pytest
import time

from ydb.tests.sql.lib.test_query import Query
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.multicluster_test_base import MulticlusterTestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_four_sync, index_zero_sync


class TestAsyncReplication(MulticlusterTestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            # ("table_index_4_UNIQUE_SYNC", pk_types, {},
            # index_four_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_3_UNIQUE_SYNC", pk_types, {},
            # index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            # ("table_index_2_UNIQUE_SYNC", pk_types, {},
            # index_second_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_1_UNIQUE_SYNC", pk_types, {},
            # index_first_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_0_UNIQUE_SYNC", pk_types, {},
            # index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
            ("table_all_types", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", ""),
        ]
    )
    def test_async_replication(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml_cluster_1 = DMLOperations(Query.create(
            self.get_database(), self.get_endpoint(self.clusters[0])))
        dml_cluster_2 = DMLOperations(Query.create(
            self.get_database(), self.get_endpoint(self.clusters[1])))

        dml_cluster_1.create_table(table_name, pk_types, all_types,
                                   index, ttl, unique, sync)
        dml_cluster_1.insert(table_name, all_types, pk_types, index, ttl)
        dml_cluster_2.query(f"""
                        CREATE ASYNC REPLICATION `replication_{table_name}`
                        FOR `{self.get_database()}/{table_name}` AS `{self.get_database()}/{table_name}`
                        WITH (
                        CONNECTION_STRING = 'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}'
                            )
                         """)
        for _ in range(100):
            try:
                dml_cluster_2.query(
                    f"select count(*) as count from {table_name}")
                break
            except Exception:
                time.sleep(1)
        dml_cluster_2.select_after_insert(
            table_name, all_types, pk_types, index, ttl)
        dml_cluster_1.query(f"delete from {table_name}")
        assert wait_for(self.create_predicate(True, table_name, dml_cluster_2.query),
                        timeout_seconds=100) is True, "Expected zero rows after delete"
        dml_cluster_1.insert(table_name, all_types, pk_types, index, ttl)
        wait_for(self.create_predicate(False, table_name,
                 dml_cluster_2.query), timeout_seconds=100)
        dml_cluster_2.select_after_insert(
            table_name, all_types, pk_types, index, ttl)

    def create_predicate(self, is_zero, table_name, dml_cluster):
        def predicate():
            rows = dml_cluster(
                f"select count(*) as count from {table_name}")
            if is_zero:
                return len(rows) == 1 and rows[0].count == 0
            else:
                return len(rows) == 1 and rows[0].count != 0
        return predicate
