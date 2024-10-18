# -*- coding: utf-8 -*-
import logging
import time
from hamcrest import assert_that

from ydb.core.protos.cms_pb2 import EAvailabilityMode

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.protobuf import KVRequest
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start
from ydb.tests.library.matchers.response import is_ok_response

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
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()
        # CMS will not let disable state storage
        # nodes for first 2 minutes
        time.sleep(120)
        cms.request_increase_ratio_limit(cls.cluster.client)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestCmsDegradedGroups(AbstractLocalClusterTest):
    def test_no_degraded_groups_after_shutdown(self):
        number_of_tablets = 10
        tablet_ids = create_tablets_and_wait_for_start(
            self.cluster.client, number_of_tablets,
            batch_size=number_of_tablets,
            timeout_seconds=120
        )

        allowed_hosts = cms.request_shutdown_nodes(self.cluster.client, self.cluster.nodes.keys(), type(self).mode)
        for node in allowed_hosts:
            self.cluster.nodes[node].stop()

        client = utils.create_client_from_alive_hosts(self.cluster, allowed_hosts)
        # if there are no degraded groups
        # then write returns ok
        for tablet_id in tablet_ids:
            resp = client.kv_request(
                tablet_id, KVRequest().write(bytes("key", 'utf-8'), bytes(utils.value_for("key", tablet_id), 'utf-8'))
            )
            assert_that(resp, is_ok_response())


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
