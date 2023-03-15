# -*- coding: utf-8 -*-
import os
import logging

from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.types import Erasure


logger = logging.getLogger(__name__)


class TestOn3DC(object):
    @classmethod
    def setup_class(cls):
        config_generator = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=2,
            n_to_select=1,
            additional_log_configs={
                'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                'SCHEMESHARD_DESCRIBE': LogLevels.DEBUG,

                'SCHEME_BOARD_POPULATOR': LogLevels.DEBUG,
                'SCHEME_BOARD_REPLICA': LogLevels.DEBUG,
            }
        )
        cls.cluster = kikimr_cluster_factory(
            config_generator
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_create_dirs(self):
        return
        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database="/Root")

        rounds = 2
        for restart_no in range(rounds):
            if restart_no != 0:
                for node in self.cluster.nodes.values():
                    node.start()

            client = ydb.Driver(driver_config)
            client.wait(120)

            for iNo in range(100):
                dir_name = "{}_{}".format(restart_no, iNo)
                logger.debug("create directory %s", dir_name)
                result = client.scheme_client.make_directory(
                    os.path.join(
                        '/', 'Root', dir_name
                    )
                )
                logger.debug("result %s", str(result))

            if restart_no != rounds-1:
                for node in self.cluster.nodes.values():
                    node.stop()
