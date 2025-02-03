import logging
import time

from hamcrest import assert_that

from ydb.core.protos.cms_pb2 import EAvailabilityMode
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms

from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.tests.library.common.delayed import wait_tablets_are_active

import utils

logger = logging.getLogger(__name__)


class AbstractLocalClusterTest(object):
    erasure = None
    mode = None

    @classmethod
    def setup_class(cls):
        nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        configurator = KikimrConfigGenerator(cls.erasure,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             additional_log_configs={'CMS': LogLevels.DEBUG},
                                             )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()

        time.sleep(120)
        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestCmsStateStorageRestarts(AbstractLocalClusterTest):
    def test_restart_as_much_as_can(self):
        number_of_tablets = 20

        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.cluster.scheme_client.make_directory(path)
        tablet_ids = create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, number_of_tablets, table_path, timeout_seconds=120)
        restart_nodes = cms.request_shutdown_as_much_as_possible(self.cluster.client, self.cluster.nodes.keys(), type(self).mode)

        for node in restart_nodes:
            self.cluster.nodes[node].stop()

        client = utils.create_client_from_alive_hosts(self.cluster, restart_nodes)
        kv_client = utils.create_kv_client_from_alive_hosts(self.cluster, restart_nodes)
        for tablet_id in tablet_ids:
            client.tablet_kill(tablet_id)

        for partition_id, tablet_id in enumerate(tablet_ids):
            resp = kv_client.kv_write(table_path, partition_id, "key", utils.value_for("key", tablet_id))
            assert_that(resp.operation.status, StatusIds.SUCCESS)

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
