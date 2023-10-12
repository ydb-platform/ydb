import logging
import time

from hamcrest import assert_that

from ydb.core.protos.cms_pb2 import EAvailabilityMode

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.protobuf import KVRequest
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start
from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.tests.library.matchers.response import is_ok_response

import utils

logger = logging.getLogger(__name__)


class AbstractLocalClusterTest(object):
    erasure = None
    mode = None

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        nodes_count *= 2
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             additional_log_configs={'CMS': LogLevels.DEBUG},
                                             )
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()

        time.sleep(120)
        cms.request_increase_ratio_limit(cls.cluster.client)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestCmsStateStorageRestarts(AbstractLocalClusterTest):
    def test_restart_as_much_as_can(self):
        number_of_tablets = 20
        tablet_ids = create_tablets_and_wait_for_start(
            self.cluster.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=120
        )

        restart_nodes = cms.request_shutdown_as_much_as_possible(self.cluster.client, self.cluster.nodes.keys(), type(self).mode)

        for node in restart_nodes:
            self.cluster.nodes[node].stop()

        client = utils.create_client_from_alive_hosts(self.cluster, restart_nodes)

        for tablet_id in tablet_ids:
            client.tablet_kill(tablet_id)

        for tablet_id in tablet_ids:
            resp = client.kv_request(
                tablet_id, KVRequest().write(bytes("key", 'utf-8'), bytes(utils.value_for("key", tablet_id), 'utf-8'))
            )
            assert_that(resp, is_ok_response())

        for node in restart_nodes:
            self.cluster.nodes[node].start()

        wait_tablets_are_active(self.cluster.client, tablet_ids)


class TestCmsStateStorageRestartsBlockMax(AbstractTestCmsStateStorageRestarts):
    erasure = Erasure.BLOCK_4_2
    mode = EAvailabilityMode.MODE_MAX_AVAILABILITY


class TestCmsStateStorageRestartsBlockKeep(AbstractTestCmsStateStorageRestarts):
    erasure = Erasure.BLOCK_4_2
    mode = EAvailabilityMode.MODE_KEEP_AVAILABLE


class TestCmsStateStorageRestartsMirrorMax(AbstractTestCmsStateStorageRestarts):
    erasure = Erasure.MIRROR_3_DC
    mode = EAvailabilityMode.MODE_MAX_AVAILABILITY


class TestCmsStateStorageRestartsMirrorKeep(AbstractTestCmsStateStorageRestarts):
    erasure = Erasure.MIRROR_3_DC
    mode = EAvailabilityMode.MODE_KEEP_AVAILABLE
