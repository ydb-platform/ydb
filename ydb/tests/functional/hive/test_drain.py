# -*- coding: utf-8 -*-
import logging
from ydb.public.api.protos import ydb_keyvalue_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import yatest
from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.common.types import Erasure, TabletStates
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness import param_constants
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start


logger = logging.getLogger(__name__)
TabletsCount = 1500
TimeoutSeconds = 300

# A tablet that has reached any of these states has already superseded its previous
# generation: it locked the state storage with a greater generation, which makes the
# guardian of the previous leader demote it.
AliveTabletStates = frozenset((
    TabletStates.Candidate,
    TabletStates.BlockBlobStorage,
    TabletStates.RebuildGraph,
    TabletStates.WriteZeroEntry,
    TabletStates.Restored,
    TabletStates.Active,
))


class TestHive(object):
    erasure = Erasure.BLOCK_4_2
    value = b"drain-persistent-value" * 4096

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                cls.erasure,
                use_in_memory_pdisks=False,
                hive_config={
                    'max_node_usage_to_kick': 100,
                    'min_scatter_to_balance': 100,
                    'min_counter_scatter_to_balance': 100,
                    'object_imbalance_to_balance': 100,
                },
                extra_feature_flags=['enable_drain_on_shutdown'],
                shutdown_config={'drain_timeout_seconds': 1000000},
            )
        )
        cls.cluster.start()
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def wait_storage_ready(self):
        def check():
            config = self.cluster.client.query_base_config().BaseConfig
            connected_nodes = {node.NodeId for node in config.Node if node.Connected}
            assert set(self.cluster.nodes).issubset(connected_nodes), 'Storage nodes are not connected'
            assert config.VSlot, 'Storage groups are not configured'
            assert all(slot.Ready for slot in config.VSlot), 'Storage disks are not BSC-ready'

        retry_assertions(check, timeout_seconds=TimeoutSeconds)

    def write_probe(self, table_path):
        response = self.cluster.kv_client.kv_write(table_path, 0, 'persistent-key', self.value)
        assert response.operation.status == StatusIds.SUCCESS

    def check_probe(self, table_path):
        response = self.cluster.kv_client.kv_read(table_path, 0, 'persistent-key')
        assert response.operation.status == StatusIds.SUCCESS
        result = ydb_keyvalue_pb2.ReadResult()
        assert response.operation.result.Unpack(result)
        assert result.value == self.value

    def test_drain_tablets(self):
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.cluster.scheme_client.make_directory(path)
        all_tablet_ids = create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, TabletsCount, table_path)
        self.wait_storage_ready()
        self.write_probe(table_path)
        for node_id, node in self.cluster.nodes.items():
            if node_id == 1:
                continue

            logger.info("Started tablets drain")

            try:
                yatest.common.execute(
                    [
                        param_constants.kikimr_driver_path(), '--mb-total-timeout', str(TimeoutSeconds),
                        '-s', 'localhost:%d' % node.grpc_port, 'admin', 'node', str(node_id), 'drain'
                    ]
                )

            except yatest.common.ExecutionError:
                pass

            node.stop()

            wait_tablets_are_active(
                self.cluster.client,
                all_tablet_ids,
            )

            self.check_probe(table_path)
            node.start()
            self.wait_storage_ready()
            self.check_probe(table_path)

    def test_drain_on_stop(self):
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.cluster.scheme_client.make_directory(path)
        all_tablet_ids = create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, TabletsCount, table_path)
        self.wait_storage_ready()
        self.write_probe(table_path)
        for node_id, node in self.cluster.nodes.items():
            if node_id == 1:
                continue

            logger.info(f"Stopping node {node_id}")

            node.stop()

            if not node.killed:
                wait_tablets_are_active(
                    self.cluster.client,
                    all_tablet_ids,
                    0,
                    active_states=AliveTabletStates,
                )

            self.check_probe(table_path)
            node.start()
            self.wait_storage_ready()
            self.check_probe(table_path)


class TestHiveBlock82(TestHive):
    erasure = Erasure.BLOCK_8_2
