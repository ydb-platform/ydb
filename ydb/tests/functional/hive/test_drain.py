# -*- coding: utf-8 -*-
import logging

import yatest
from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness import param_constants
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start


logger = logging.getLogger(__name__)
TabletsCount = 1500
TimeoutSeconds = 300


class TestHive(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                Erasure.BLOCK_4_2,
                additional_log_configs={
                    'HIVE': LogLevels.TRACE, 'LOCAL': LogLevels.TRACE
                },
                use_in_memory_pdisks=False,
                hive_config={
                    'max_node_usage_to_kick': 100,
                    'min_scatter_to_balance': 100,
                    'min_counter_scatter_to_balance': 100,
                    'object_imbalance_to_balance': 100,
                },
            )
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_drain_tablets(self):
        all_tablet_ids = create_tablets_and_wait_for_start(self.cluster.client, TabletsCount, batch_size=TabletsCount)
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

            node.start()

    def test_drain_on_stop(self):
        all_tablet_ids = create_tablets_and_wait_for_start(self.cluster.client, TabletsCount, batch_size=TabletsCount)
        for node_id, node in self.cluster.nodes.items():
            if node_id == 1:
                continue

            logger.info(f"Stopping node {node_id}")

            node.stop()

            if not node.killed:
                wait_tablets_are_active(
                    self.cluster.client,
                    all_tablet_ids,
                    0,  # Tablets should already be active, as drain must be finished at this point
                )

            node.start()
