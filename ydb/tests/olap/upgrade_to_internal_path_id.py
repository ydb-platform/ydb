import pytest
import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestUpgradeToInternalPathId:
    cluster = None
    session = None
    partition_count = 17
    num_rows = 1000
    config = KikimrConfigGenerator(
        use_in_memory_pdisks=False,
        column_shard_config={"generate_internal_path_id": False}
    )

    @pytest.fixture(autouse=True)
    def setup(self):
        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        self.session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)

        yield

        self.session.stop()
        self.cluster.stop()

    def restart_cluster(self, generate_internal_path_id):
        self.config.yaml_config["column_shard_config"]["generate_internal_path_id"] = generate_internal_path_id
        self.cluster.update_configurator_and_restart(self.config)
        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root")
        self.session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)

    def create_table_with_data(self, table_name):
        self.session.execute_with_retries(f"""
                CREATE TABLE `{table_name}` (
                    k Int32 NOT NULL,
                    v String,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN, PARTITION_COUNT = {self.partition_count}    )
            """)
        self.session.execute_with_retries(f"""
            $keys = ListFromRange(0, {self.num_rows});
            $rows = ListMap($keys, ($i)->(<|k:$i, v: "value_" || CAST($i as String)|>));
            INSERT INTO `{table_name}`
            SELECT * FROM AS_TABLE($rows);
            """)

    def validate_table(self, table_name):
        result = self.session.execute_with_retries(f"""
            SELECT sum(k) AS c FROM `{table_name}`
        """)
        assert result[0].rows[0]["c"] == self.num_rows * (self.num_rows - 1) / 2

    def get_path_ids(self, table_name):
        result = self.session.execute_with_retries(f"""
            SELECT TabletId, PathId, InternalPathId FROM `{table_name}/.sys/primary_index_granule_stats`
        """)
        rows = [row for result_set in result for row in result_set.rows]
        result = {}
        for row in rows:
            internalPathId = row["InternalPathId"]
            pathId = row["PathId"]
            if internalPathId not in result:
                result[internalPathId] = {}
            if pathId not in result[internalPathId]:
                result[internalPathId][pathId] = 0
            result[internalPathId][pathId] += 1
        return result

    def test(self):
        self.create_table_with_data("table1")
        self.validate_table("table1")
        table1PathMapping = self.get_path_ids("table1")
        assert len(table1PathMapping) == 1
        table1InternalPathId = next(iter(table1PathMapping))
        assert len(table1PathMapping[table1InternalPathId]) == 1
        table1PathId = next(iter(table1PathMapping[table1InternalPathId]))
        assert table1InternalPathId == table1PathId
        assert table1PathMapping[table1InternalPathId][table1PathId] == self.partition_count

        # restart using another configuration
        self.restart_cluster(generate_internal_path_id=True)
        self.validate_table("table1")
        assert table1PathMapping == self.get_path_ids("table1")

        self.create_table_with_data("table2")
        self.validate_table("table2")
        table2PathMapping = self.get_path_ids("table2")
        assert len(table2PathMapping) == self.partition_count
        table2InternalPathId = next(iter(table2PathMapping))
        assert table2InternalPathId not in table2PathMapping[table2InternalPathId]

        # restart using the same configuration as before
        self.restart_cluster(generate_internal_path_id=True)
        self.validate_table("table1")
        assert table1PathMapping == self.get_path_ids("table1")
        self.validate_table("table2")
        assert table2PathMapping == self.get_path_ids("table2")
