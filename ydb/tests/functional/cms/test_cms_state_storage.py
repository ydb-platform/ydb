import logging
import time

from ydb.core.protos.cms_pb2 import EAvailabilityMode

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start
from ydb.tests.library.common.delayed import wait_tablets_are_active

import utils

logger = logging.getLogger(__name__)


class AbstractLocalClusterTest(object):
    erasure = None
    mode = None

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(Erasure.NONE,
                                             nodes=27,
                                             use_in_memory_pdisks=False,
                                             additional_log_configs={'CMS': LogLevels.DEBUG},
                                             state_storage_rings=[[n, n + 1, n + 2] for n in range(1, 27, 3)]
                                             )
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()

        time.sleep(120)
        cms.request_increase_ratio_limit(cls.cluster.client)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestCmsStateStorageSimple(AbstractLocalClusterTest):
    def test_check_shutdown_state_storage_nodes(self):
        number_of_tablets = 10
        tablet_ids = create_tablets_and_wait_for_start(
            self.cluster.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=120
        )

        allowed_hosts = cms.request_shutdown_nodes(self.cluster.client,
                                                   self.cluster.nodes.keys(),
                                                   type(self).mode)

        for node in allowed_hosts:
            self.cluster.nodes[node].stop()

        client = utils.create_client_from_alive_hosts(self.cluster, allowed_hosts)

        for tablet_id in tablet_ids:
            client.tablet_kill(tablet_id)

        wait_tablets_are_active(client, tablet_ids)


class TestCmsStateStorageSimpleKeep(AbstractTestCmsStateStorageSimple):
    mode = EAvailabilityMode.MODE_KEEP_AVAILABLE


class TestCmsStateStorageSimpleMax(AbstractTestCmsStateStorageSimple):
    mode = EAvailabilityMode.MODE_MAX_AVAILABILITY
