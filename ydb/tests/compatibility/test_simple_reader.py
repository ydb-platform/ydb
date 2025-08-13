import pytest

from datetime import datetime, timedelta
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, MixedClusterFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class SimpleReaderWorkload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.table_name = "/Root/simple_reader_table"
        self.batch_size = 1000

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                    CREATE TABLE `{self.table_name}` (
                        id Uint64 NOT NULL,
                        timestamp Timestamp NOT NULL,
                        value Uint64,
                        data Float,
                        PRIMARY KEY (id, timestamp)
                    ) WITH (
                        STORE = COLUMN
                    );
                """
            )

    def write_data_batch(self, start_id, batch_size):
        rows = []
        current_time = datetime.now()
        for i in range(batch_size):
            row = {
                "id": start_id + i,
                "timestamp": current_time + timedelta(seconds=i),
                "value": start_id + i,
                "data": float(start_id + i) * 1.5,
            }

            rows.append(row)

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("timestamp", ydb.PrimitiveType.Timestamp)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)
        column_types.add_column("data", ydb.PrimitiveType.Float)
        self.driver.table_client.bulk_upsert(self.table_name, rows, column_types)
        return len(rows)

    def write_data_with_overlaps(self):
        total_written = 0
        base_id = 0
        base_time = datetime.now()
        
        for batch_num in range(6):
            rows = []
            current_time = base_time + timedelta(seconds=batch_num * 100)
            for i in range(self.batch_size):
                overlap_id = base_id + i
                row = {
                    "id": overlap_id,
                    "timestamp": current_time + timedelta(seconds=i),
                    "value": overlap_id,
                    "data": float(overlap_id) * 1.5 + batch_num,
                }

                rows.append(row)

            column_types = ydb.BulkUpsertColumns()
            column_types.add_column("id", ydb.PrimitiveType.Uint64)
            column_types.add_column("timestamp", ydb.PrimitiveType.Timestamp)
            column_types.add_column("value", ydb.PrimitiveType.Uint64)
            column_types.add_column("data", ydb.PrimitiveType.Float)
            self.driver.table_client.bulk_upsert(self.table_name, rows, column_types)
            total_written += len(rows)

        return total_written

    def execute_scan_query(self, query_body):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(query_body)
            return result[0].rows

    def test_simple_reader_queries(self):
        queries = [
            f"SELECT COUNT(*) as count FROM `{self.table_name}`",
            f"SELECT SUM(value) as sum_value FROM `{self.table_name}`",
            f"SELECT AVG(data) as avg_data FROM `{self.table_name}`",
            f"SELECT MIN(timestamp) as min_time, MAX(timestamp) as max_time FROM `{self.table_name}`",
            f"SELECT id, value, data FROM `{self.table_name}` WHERE value > 1000 LIMIT 100",
            f"SELECT id, timestamp, value FROM `{self.table_name}` ORDER BY timestamp DESC LIMIT 50",
        ]

        results = []
        for query in queries:
            try:
                result = self.execute_scan_query(query)
                results.append((query, result, True))
            except Exception as e:
                results.append((query, str(e), False))

        return results

    def verify_data_consistency(self):
        count_result = self.execute_scan_query(f"SELECT COUNT(*) as count FROM `{self.table_name}`")
        total_count = count_result[0]["count"]
        sum_result = self.execute_scan_query(f"SELECT SUM(value) as sum_value FROM `{self.table_name}`")
        total_sum = sum_result[0]["sum_value"]
        unique_count_result = self.execute_scan_query(f"SELECT COUNT(DISTINCT id) as unique_count FROM `{self.table_name}`")
        unique_count = unique_count_result[0]["unique_count"]
        
        expected_total_count = 6 * self.batch_size
        expected_unique_count = self.batch_size
        expected_total_sum = sum(range(self.batch_size)) * 6
        
        is_consistent = (total_count == expected_total_count and 
                        unique_count == expected_unique_count and 
                        total_sum == expected_total_sum)

        return {
            "total_count": total_count,
            "total_sum": total_sum,
            "unique_count": unique_count,
            "expected_total_count": expected_total_count,
            "expected_unique_count": expected_unique_count,
            "expected_total_sum": expected_total_sum,
            "is_consistent": is_consistent,
        }


class TestSimpleReaderMixedCluster(MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "disabled_on_scheme_shard": False,
                "compaction_enabled": False,
            },
        )

    def test_simple_reader_mixed_cluster(self):
        workload = SimpleReaderWorkload(self.driver, self.endpoint)
        workload.create_table()
        total_written = workload.write_data_with_overlaps()
        assert total_written > 0
        consistency = workload.verify_data_consistency()
        assert consistency["is_consistent"], f"Data consistency check failed: {consistency}"
        query_results = workload.test_simple_reader_queries()
        failed_queries = [(q, r) for q, r, success in query_results if not success]
        assert len(failed_queries) == 0


class TestSimpleReaderRestartToAnotherVersion(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "disabled_on_scheme_shard": False,
                "compaction_enabled": False,
            },
        )

    def test_simple_reader_version_upgrade(self):
        workload = SimpleReaderWorkload(self.driver, self.endpoint)
        workload.create_table()
        workload.write_data_with_overlaps()
        initial_consistency = workload.verify_data_consistency()
        assert initial_consistency["is_consistent"], f"Initial data consistency check failed: {initial_consistency}"
        self.change_cluster_version()
        workload.write_data_with_overlaps()
        final_consistency = workload.verify_data_consistency()
        assert final_consistency["is_consistent"], f"Final data consistency check failed: {final_consistency}"
        query_results = workload.test_simple_reader_queries()
        failed_queries = [(q, r) for q, r, success in query_results if not success]
        assert len(failed_queries) == 0, f"Failed queries: {failed_queries}"
        assert final_consistency["total_count"] == final_consistency["expected_total_count"], f"Total count mismatch: got {final_consistency['total_count']}, expected {final_consistency['expected_total_count']}"
        assert final_consistency["unique_count"] == final_consistency["expected_unique_count"], f"Unique count mismatch: got {final_consistency['unique_count']}, expected {final_consistency['expected_unique_count']}"
        assert final_consistency["total_sum"] == final_consistency["expected_total_sum"], f"Total sum mismatch: got {final_consistency['total_sum']}, expected {final_consistency['expected_total_sum']}"


class TestSimpleReaderTabletTransfer(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "disabled_on_scheme_shard": False,
                "compaction_enabled": False,
            },
        )

    def test_simple_reader_tablet_transfer(self):
        workload = SimpleReaderWorkload(self.driver, self.endpoint)
        workload.create_table()
        total_written = 0
        for _ in range(8):
            written = workload.write_data_with_overlaps()
            total_written += written

        initial_consistency = workload.verify_data_consistency()
        assert initial_consistency["is_consistent"], f"Initial data consistency check failed: {initial_consistency}"
        
        step_count = 0
        for _ in self.roll():
            if step_count >= 8:
                break
            workload.write_data_with_overlaps()
            step_count += 1

        final_consistency = workload.verify_data_consistency()
        assert final_consistency["is_consistent"], f"Final data consistency check failed: {final_consistency}"
        query_results = workload.test_simple_reader_queries()
        failed_queries = [(q, r) for q, r, success in query_results if not success]
        assert len(failed_queries) == 0
