import pytest
import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
import logging

logger = logging.getLogger(__name__)

class TestUpgradeToInternalPathId:
    cluster = None
    session = None
    partition_count = 17
    num_rows = 1000
    config = None

    @pytest.fixture(autouse=True)
    def setup(self):
        self.config = KikimrConfigGenerator(
            use_in_memory_pdisks=False,
            column_shard_config={"generate_internal_path_id": False}
        )

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
        driver.wait(20)
        self.session = ydb.QuerySessionPool(driver)

    def create_table_with_data(self, table_name, start_value):
        self.session.execute_with_retries(f"""
                CREATE TABLE `{table_name}` (
                    k Int32 NOT NULL,
                    v String,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN, PARTITION_COUNT = {self.partition_count}    )
            """)
        self.session.execute_with_retries(f"""
            $keys = ListFromRange({start_value}, {start_value} + {self.num_rows});
            $rows = ListMap($keys, ($i)->(<|k:$i, v: "value_" || CAST($i as String)|>));
            INSERT INTO `{table_name}`
            SELECT * FROM AS_TABLE($rows);
            """)

    def validate_table(self, table_name, start_value):
        result = self.session.execute_with_retries(f"""
            SELECT sum(k) AS c FROM `{table_name}`
        """)
        assert result[0].rows[0]["c"] == (2 * start_value + self.num_rows - 1) * self.num_rows / 2

    def get_path_ids(self, table_name):
        result = self.session.execute_with_retries(f"""
            SELECT TabletId, PathId, InternalPathId FROM `{table_name}/.sys/primary_index_granule_stats`
        """)
        rows = [row for result_set in result for row in result_set.rows]
        mapping = {}
        for row in rows:
            external_path_id = row["PathId"]
            internal_path_id = row["InternalPathId"]
            if external_path_id not in mapping:
                mapping[external_path_id] = {}
            if internal_path_id not in mapping[external_path_id]:
                mapping[external_path_id][internal_path_id] = 0
            mapping[external_path_id][internal_path_id] += 1
        assert len(mapping) == 1
        external_path_id = next(iter(mapping))
        counted_internal_path_ids = mapping[external_path_id]
        return (external_path_id, counted_internal_path_ids)

    def test(self):
        tables_path_mapping = []
        for i in range(10):
            generate_internal_path_id = i % 2 == 1
            logger.info(f"Iteration {i}, with generate_internal_path_id={generate_internal_path_id}")
            self.restart_cluster(generate_internal_path_id=generate_internal_path_id)
            for j in range(0, i):
                existing_table_name = f"table{j}"
                self.validate_table(existing_table_name, j)
                assert j < len(tables_path_mapping)
                assert self.get_path_ids(existing_table_name) == tables_path_mapping[j]
            new_table_name = f"table{i}"
            self.create_table_with_data(new_table_name, i)
            self.validate_table(new_table_name, i)
            assert len(tables_path_mapping) == i
            path_mapping = self.get_path_ids(new_table_name)
            tables_path_mapping.append(path_mapping)
            logger.info(f"{i}, path_mapping: {path_mapping}")
            
            external_path_id, counted_internal_path_ids = path_mapping
            assert external_path_id < i + 10  # with a gap for some path ids created before tables creation

            if generate_internal_path_id:
                assert len(counted_internal_path_ids) == self.partition_count
                assert all([internal_path_id >= 1_000_000_000 for internal_path_id in counted_internal_path_ids.keys()])
                assert all([count == 1 for count in counted_internal_path_ids.values()])
            else:
                assert len(counted_internal_path_ids) == 1
                assert external_path_id in counted_internal_path_ids
                assert counted_internal_path_ids[external_path_id] == self.partition_count
