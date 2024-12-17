# -*- coding: utf-8 -*-
import logging
import time
from hamcrest import assert_that

from ydb.core.protos.cms_pb2 import EAvailabilityMode

from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_kv_tablets_and_wait_for_start
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
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
                                             additional_log_configs={'CMS': LogLevels.DEBUG}
                                             )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        # CMS will not let disable state storage
        # nodes for first 2 minutes
        time.sleep(120)
        cms.request_increase_ratio_limit(cls.cluster.client)
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.swagger_client = SwaggerClient(host, mon_port)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestCmsDegradedGroups(AbstractLocalClusterTest):
    def test_no_degraded_groups_after_shutdown(self):
        number_of_tablets = 10
        table_path = '/Root/mydb/mytable'
        tablet_ids = create_kv_tablets_and_wait_for_start(self.cluster.client, self.cluster.kv_client, self.swagger_client, number_of_tablets, table_path, timeout_seconds=120)
        allowed_hosts = cms.request_shutdown_nodes(self.cluster.client, self.cluster.nodes.keys(), type(self).mode)
        for node in allowed_hosts:
            self.cluster.nodes[node].stop()

        kv_client = utils.create_kv_client_from_alive_hosts(self.cluster, allowed_hosts)
        # if there are no degraded groups
        # then write returns ok
        for partition_id, tablet_id in enumerate(tablet_ids):
            resp = kv_client.kv_write(table_path, partition_id, "key", utils.value_for("key", tablet_id))
            assert_that(resp.operation.status == StatusIds.SUCCESS)


class TestDegradedGroupBlock42Max(AbstractTestCmsDegradedGroups):
    erasure = Erasure.BLOCK_4_2
    mode = EAvailabilityMode.MODE_MAX_AVAILABILITY


class TestDegradedGroupBlock42Keep(AbstractTestCmsDegradedGroups):
    erasure = Erasure.BLOCK_4_2
    mode = EAvailabilityMode.MODE_KEEP_AVAILABLE


class TestDegradedGroupMirror3dcMax(AbstractTestCmsDegradedGroups):
    erasure = Erasure.MIRROR_3_DC
    mode = EAvailabilityMode.MODE_MAX_AVAILABILITY


class TestDegradedGroupMirror3dcKeep(AbstractTestCmsDegradedGroups):
    erasure = Erasure.MIRROR_3_DC
    mode = EAvailabilityMode.MODE_KEEP_AVAILABLE
