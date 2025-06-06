# -*- coding: utf-8 -*-
import logging
import time
import pytest
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.types import TabletStates, TabletTypes
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RollingUpgradeAndDowngradeFixture


logger = logging.getLogger(__name__)


class TestFollowersCompatibility(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        self.datacenters = [1, 2, 3]
        self.dc_map = {i : self.datacenters[(i - 1) % 3] for i in range(1, 10)}
        self.dc_map[0] = "NO DC"
        yield from self.setup_cluster(
            dc_mapping=self.dc_map,
            additional_log_configs={'HIVE': LogLevels.DEBUG},
            use_in_memory_pdisks=False
        )

    def check_followers(self, node_idx=1):
        client = SwaggerClient(self.cluster.nodes[node_idx].host, self.cluster.nodes[node_idx].mon_port)
        try:
            data = client.tablet_info()
        except Exception:
            return False, "could not connect"
        if not data:
            return False, "no answer from server"
        tablet_info = data['TabletStateInfo']
        tablet_to_dc = dict()
        hive_node = 0
        for tablet in tablet_info:
            logger.debug(f"tablet_info: {tablet}")
            if tablet.get('FollowerId', 0) == 0:
                if tablet['Type'] == int(TabletTypes.FLAT_HIVE):
                    hive_node = tablet['NodeId']
                    if tablet['State'] != TabletStates.Active:
                        return False, "hive is down"
                continue
            if tablet['State'] != TabletStates.Active:
                continue
            tablet_id = tablet['TabletId']
            if tablet_id not in tablet_to_dc:
                tablet_to_dc[tablet_id] = []
            tablet_to_dc[tablet_id].append(self.dc_map[tablet['NodeId']])
        if hive_node == 0:
            return False, "hive is down"
        for tablet_id, data_centers in tablet_to_dc.items():
            if self.config.get_binary_path(hive_node) == kikimr_driver_path():
                if len(set(data_centers)) != len(self.datacenters) or len(data_centers) != len(self.datacenters):
                    msg = f"datacenters for tablet {tablet_id} are {data_centers}, hive on node {hive_node} - new version"
                    logger.info(msg)
                    return False, msg
            else:  # A very relaxed check for old hive version
                if len(data_centers) > len(self.datacenters):
                    msg = f"datacenters for tablet {tablet_id} are {data_centers}, hive on node {hive_node} - old version"
                    logger.info(msg)
                    return False, msg
        return True, "ok"

    def test_followers_compatability(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    """create table `sample_table` (
                        id Uint64,
                        value Uint64,
                        payload Utf8,
                        PRIMARY KEY(id)
                      )
                      WITH (
                        AUTO_PARTITIONING_BY_SIZE = ENABLED,
                        AUTO_PARTITIONING_PARTITION_SIZE_MB = 1,
                        READ_REPLICAS_SETTINGS = \"PER_AZ:1\"
                      );"""
                )
                id_ = 0

                upsert_count = 4  # per iteration
                iteration_count = 20
                # Simulate some load with dc outages, so that:
                # - Hive restarts and runs on different ydb versions
                # - Tablets are splitting
                # - Number of followers is changing
                for i in range(iteration_count):
                    for node_id, node in self.cluster.nodes.items():
                        if node.data_center == self.datacenters[i % len(self.datacenters)]:
                            node.stop()
                    rows = []
                    for j in range(upsert_count):
                        row = {}
                        row["id"] = id_
                        row["value"] = 1
                        row["payload"] = "DEADBEEF" * 1024 * 256
                        rows.append(row)
                        id_ += 1

                    column_types = ydb.BulkUpsertColumns()
                    column_types.add_column("id", ydb.PrimitiveType.Uint64)
                    column_types.add_column("value", ydb.PrimitiveType.Uint64)
                    column_types.add_column("payload", ydb.PrimitiveType.Utf8)
                    try:
                        self.driver.table_client.bulk_upsert(
                            "Root/sample_table", rows, column_types
                        )
                    except Exception as e:
                        logger.error(e)

                    for node_id, node in self.cluster.nodes.items():
                        if node.data_center == self.datacenters[i % len(self.datacenters)]:
                            node.start()
                    retry_count = 0
                    backoff = .1
                    while True:
                        retry_count += 1
                        logger.info(f"check_followers: iteration {i}, try {retry_count}")
                        ok, msg = self.check_followers()
                        if retry_count == 5:
                            assert ok, msg
                        if ok:
                            break
                        time.sleep(backoff)
                        backoff *= 2


class TestSecondaryIndexFollowers(RollingUpgradeAndDowngradeFixture):
    TABLE_NAME = "table"
    INDEX_NAME = "idx"
    ATTEMPT_COUNT = 10
    ATTEMPT_INTERVAL = 5

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_follower_stats": True
            }
        )

    def create_table(self, enable_followers):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            create_table_query = f"""
                CREATE TABLE `{self.TABLE_NAME}` (
                    key Int64 NOT NULL,
                    subkey Int64 NOT NULL,
                    value Utf8 NOT NULL,
                    PRIMARY KEY (key)
                );

                ALTER TABLE `{self.TABLE_NAME}` ADD INDEX `{self.INDEX_NAME}` GLOBAL ASYNC ON (`subkey`) COVER (`value`);
            """
            session_pool.execute_with_retries(create_table_query)

            if enable_followers:
                alter_index_query = f"""
                    ALTER TABLE `{self.TABLE_NAME}` ALTER INDEX `{self.INDEX_NAME}` SET READ_REPLICAS_SETTINGS "PER_AZ:1";
                """
                session_pool.execute_with_retries(alter_index_query)

    def write_data(self):
        def operation(session):
            for key in range(100):
                session.transaction().execute(
                    f"""
                    UPSERT INTO {self.TABLE_NAME} (key, subkey, value) VALUES ({key}, {key // 10}, 'Hello, YDB {key}!')
                    """,
                    commit_tx=True
                )

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.retry_operation_sync(operation)

    def read_data(self):
        def operation(session):
            for key in range(100):
                session.transaction(ydb.QueryStaleReadOnly()).execute(
                    f"""
                    SELECT * FROM `{self.TABLE_NAME}` VIEW `{self.INDEX_NAME}` WHERE subkey == {key // 10};
                    """,
                    commit_tx=True
                )

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.retry_operation_sync(operation)

    def check_statistics(self, enable_followers):
        queries = [
            f"""
                SELECT *
                FROM `/Root/.sys/partition_stats`
                WHERE
                    FollowerId {"!=" if enable_followers else "=="} 0
                    AND (RowReads != 0 OR RangeReads != 0)
                    AND Path = '/Root/{self.TABLE_NAME}/{self.INDEX_NAME}/indexImplTable'
            """
        ]

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for _ in range(self.ATTEMPT_COUNT):
                for query in queries:
                    result_sets = session_pool.execute_with_retries(query)
                    result_row_count = len(result_sets[0].rows)
                    if result_row_count > 0:
                        return
                time.sleep(self.ATTEMPT_INTERVAL)
            assert False, f"Expected reads but there is timeout waiting for read stats from '/Root/{self.TABLE_NAME}/{self.INDEX_NAME}/indexImplTable'"

    @pytest.mark.parametrize("enable_followers", [True, False])
    def test_secondary_index_followers(self, enable_followers):
        self.create_table(enable_followers)

        for _ in self.roll():
            self.write_data()
            self.read_data()
            self.check_statistics(enable_followers)
