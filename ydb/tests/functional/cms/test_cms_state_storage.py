import logging
import time

from ydb.core.protos.cms_pb2 import EAvailabilityMode

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
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
        configurator = KikimrConfigGenerator(Erasure.NONE,
                                             nodes=5,
                                             use_in_memory_pdisks=False,
                                             additional_log_configs={'CMS': LogLevels.DEBUG},
                                             state_storage_rings=list(range(1, 6)),
                                             n_to_select=5,
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


class AbstractTestCmsStateStorageSimple(AbstractLocalClusterTest):
    def test_check_shutdown_state_storage_nodes(self):
        number_of_tablets = 10
        path = '/Root/mydb'
        table_path = '/Root/mydb/mytable'
        self.cluster.scheme_client.make_directory(path)
        tablet_ids = create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, number_of_tablets, table_path, timeout_seconds=120)
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
