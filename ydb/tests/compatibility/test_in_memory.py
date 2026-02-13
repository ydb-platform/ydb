import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, MixedClusterFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class InMemoryWorkload:
    def __init__(self, fixture):
        self.fixture = fixture
        self.initially_in_memory_table = "/Root/initially_in_memory_table"
        self.initially_regular_table = "/Root/initially_regular_table"
        self.rows_to_insert = 1001

    def create_tables(self):
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self.initially_in_memory_table}` (
                    key Uint64 NOT NULL,
                    value_int Int64 NOT NULL,
                    value_str String NOT NULL,
                    PRIMARY KEY (key),
                    FAMILY default (
                        CACHE_MODE = "in_memory"
                    )
                );
            """)
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self.initially_regular_table}` (
                    key Uint64 NOT NULL,
                    value_int Int64 NOT NULL,
                    value_str String NOT NULL,
                    PRIMARY KEY (key)
                );
            """)

    def alter_tables(self, idx=0):
        def get_query(table_name, cache_mode):
            return f"""
                ALTER TABLE `{table_name}`
                ALTER FAMILY default
                SET CACHE_MODE "{cache_mode}";"""

        modes_to_alter = [
            (self.initially_in_memory_table, "regular" if idx % 2 == 0 else "in_memory"),
            (self.initially_regular_table, "in_memory" if idx % 2 == 0 else "regular"),
        ]
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            for table, mode in modes_to_alter:
                session_pool.execute_with_retries(get_query(table, mode))

    def write_data(self):
        rows = []
        for i in range(self.rows_to_insert):
            row = {
                "key": i,
                "value_int": i * 1000,
                "value_str": b"abcdefgh" * 2**10,
            }
            rows.append(row)

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("key", ydb.PrimitiveType.Uint64)
        column_types.add_column("value_int", ydb.PrimitiveType.Int64)
        column_types.add_column("value_str", ydb.PrimitiveType.String)

        self.fixture.driver.table_client.bulk_upsert(self.initially_in_memory_table, rows, column_types)
        self.fixture.driver.table_client.bulk_upsert(self.initially_regular_table, rows, column_types)
        return len(rows)

    def check_data(self):
        def get_query(table_name):
            return f"""
                SELECT
                    SUM(key) AS key_sum,
                    SUM(value_int) AS value_int_sum,
                    SUM(LEN(value_str)) AS value_str_sum
                FROM `{table_name}`;"""

        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            for table in [self.initially_in_memory_table, self.initially_regular_table]:
                result_sets = session_pool.execute_with_retries(get_query(table))
                assert len(result_sets) == 1
                assert len(result_sets[0].rows) == 1
                assert result_sets[0].rows[0]['key_sum'] == (self.rows_to_insert - 1) * self.rows_to_insert / 2
                assert result_sets[0].rows[0]['value_int_sum'] == (self.rows_to_insert - 1) * self.rows_to_insert / 2 * 1000
                assert result_sets[0].rows[0]['value_str_sum'] == self.rows_to_insert * 2**13


class TestInMemoryMixedCluster(MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_table_cache_modes": True,
            },
        )

    def test_in_memory_mixed_cluster(self):
        workload = InMemoryWorkload(self)
        workload.create_tables()
        total_written = workload.write_data()
        assert total_written > 0
        workload.check_data()


class TestInMemoryRestartToAnotherVersion(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_table_cache_modes": True,
            },
        )

    def test_in_memory_restart_to_version(self):
        workload = InMemoryWorkload(self)
        workload.create_tables()
        total_written = workload.write_data()
        assert total_written > 0
        workload.check_data()

        self.change_cluster_version()

        workload.alter_tables()
        workload.check_data()


class TestInMemoryRolling(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_table_cache_modes": True,
            },
        )

    def test_in_memory_rolling(self):
        workload = InMemoryWorkload(self)
        workload.create_tables()
        total_written = workload.write_data()
        assert total_written > 0
        workload.check_data()

        for i, _ in enumerate(self.roll()):
            workload.alter_tables(i)
            workload.check_data()
