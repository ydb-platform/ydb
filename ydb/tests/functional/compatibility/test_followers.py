# -*- coding: utf-8 -*-
import logging
import time
import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestFollowersCompatibility(object):
    @classmethod
    def setup_class(cls):
        last_stable_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
        binary_paths = [kikimr_driver_path(), last_stable_path]
        cls.datacenters = [1, 2, 3]
        cls.dc_map = {i : cls.datacenters[(i - 1) % 3] for i in range(1, 10)}
        cls.dc_map[0] = "NO DC"
        cls.cfg = KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC,
                                        binary_paths=binary_paths,
                                        dc_mapping=cls.dc_map,
                                        additional_log_configs={'HIVE': LogLevels.DEBUG},
                                        use_in_memory_pdisks=False)
        cls.cluster = KiKiMR(cls.cfg)
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop(kill=True)

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
            if self.cfg.get_binary_path(hive_node) == kikimr_driver_path():
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
