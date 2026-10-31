import pytest

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestIlikeNotNullMixedCluster(MixedClusterFixture):
    """ILIKE on a NOT NULL string column of a column table, pushed down to ColumnShard, in a cluster that mixes versions.

    Since #55950 KQP compiles the ignore-case UDF on a NOT NULL column with a "Just" kernel. A ColumnShard of an older
    version must still answer the query, whichever node compiles it and wherever the shards are.
    """

    table_name = "ilike_not_null"
    partitions = 12

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
        )

    def _table_path(self):
        return f"{self.database_path}/{self.table_name}".replace("//", "/")

    def _node_driver(self, node):
        driver = ydb.Driver(ydb.DriverConfig(database=self.database_path, endpoint=f"grpc://localhost:{node.port}"))
        driver.wait(timeout=60)
        return driver

    def test_ilike_not_null_from_every_node(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self._table_path()}` (
                    id Int64 NOT NULL,
                    s String NOT NULL,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = {self.partitions});
            """)
            values = ", ".join(f'({i}, "{"xfoo" if i % 3 == 0 else "FOObar" if i % 3 == 1 else "bar"}")' for i in range(120))
            session_pool.execute_with_retries(f"INSERT INTO `{self._table_path()}` (id, s) VALUES {values};")

        expected = [i for i in range(120) if i % 3 != 2]
        query = f"""
            PRAGMA OptimizeSimpleILike;
            PRAGMA AnsiLike;
            SELECT id FROM `{self._table_path()}` WHERE s ILIKE "%FOO%" ORDER BY id;
        """
        # Nodes run binaries in turn, so the query is compiled by every version and reads shards hosted by every version.
        for node_id, node in self.cluster.nodes.items():
            driver = self._node_driver(node)
            try:
                with ydb.QuerySessionPool(driver) as session_pool:
                    result = session_pool.execute_with_retries(query)
                    rows = [row["id"] for result_set in result for row in result_set.rows]
                    assert rows == expected, f"node {node_id}"
            finally:
                driver.stop()
