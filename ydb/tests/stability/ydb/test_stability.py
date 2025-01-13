# -*- coding: utf-8 -*-
import os
import time
import logging
import sys

import yatest

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.common.composite_assert import CompositeAssert # noqa
from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster # noqa
from ydb.tests.library.matchers.collection import is_empty # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


def read_table_profile():
    with open(yatest.common.source_path('ydb/tests/stability/resources/tbl_profile.txt'), 'r') as reader:
        return reader.read()


def get_slice_directory():
    return os.getenv('YDB_CLUSTER_YAML_PATH')


def get_ssh_username():
    return yatest.common.get_param("kikimr.ci.ssh_username")


def get_slice_name():
    return yatest.common.get_param("kikimr.ci.cluster_name", None)


def get_configure_binary_path():
    return yatest.common.binary_path("ydb/tools/cfg/bin/ydb_configure")


def next_version_kikimr_driver_path():
    return yatest.common.get_param("kikimr.ci.kikimr_driver_next", None)


def kikimr_driver_path():
    return yatest.common.get_param("kikimr.ci.kikimr_driver", None)


def is_deploy_cluster():
    return yatest.common.get_param("kikimr.ci.deploy_cluster", "false").lower() == "true"


class TestSetupForStability(object):
    stress_binaries_deploy_path = '/Berkanavt/nemesis/bin/'
    artifacts = (
        yatest.common.binary_path('ydb/tests/tools/nemesis/driver/nemesis'),
        yatest.common.binary_path('ydb/tests/workloads/simple_queue/simple_queue'),
        yatest.common.binary_path('ydb/tests/workloads/olap_workload/olap_workload'),
        yatest.common.binary_path('ydb/tests/workloads/statistics_workload'),
    )

    @classmethod
    def setup_class(cls):
        cls.slice_name = get_slice_name()
        assert cls.slice_name is not None

        logger.info('setup_class started for slice = {}'.format(cls.slice_name))
        cls.kikimr_cluster = ExternalKiKiMRCluster(
            kikimr_configure_binary_path=get_configure_binary_path(),
            config_path=get_slice_directory(),
            kikimr_path=kikimr_driver_path(),
            kikimr_next_path=next_version_kikimr_driver_path(),
            ssh_username=get_ssh_username(),
            deploy_cluster=is_deploy_cluster(),
        )
        cls._stop_nemesis()
        cls.kikimr_cluster.start()

        if is_deploy_cluster():
            # cleanup nemesis logs
            for node in cls.kikimr_cluster.nodes.values():
                node.ssh_command('sudo rm -rf /Berkanavt/nemesis/logs/*', raise_on_error=False)
                node.ssh_command('sudo pkill screen', raise_on_error=False)

            cls.kikimr_cluster.client.console_request(read_table_profile())
        cls.kikimr_cluster.client.update_self_heal(True)

        node = list(cls.kikimr_cluster.nodes.values())[0]
        node.ssh_command("/Berkanavt/kikimr/bin/kikimr admin console validator disable bootstrap", raise_on_error=True)
        for node in cls.kikimr_cluster.nodes.values():
            for artifact in cls.artifacts:
                node.copy_file_or_dir(
                    artifact,
                    os.path.join(
                        cls.stress_binaries_deploy_path,
                        os.path.basename(
                            artifact
                        )
                    )
                )

        logger.info('setup_class finished for slice = {}'.format(cls.slice_name))

    @classmethod
    def teardown_class(cls):
        logger.info('teardown_class started')

        if hasattr(cls, 'kikimr_cluster'):
            cls.kikimr_cluster.stop()

        logger.info('teardown_class finished')

    def setup_method(self, method=None):
        self.__start_all_cluster_nodes()

        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command('sudo pkill screen', raise_on_error=False)

    def teardown_method(self, method=None):
        self._stop_nemesis()

        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command('sudo pkill screen', raise_on_error=False)

    def __start_all_cluster_nodes(self):
        for node in self.kikimr_cluster.nodes.values():
            node.start()

    def test_simple_queue_workload(self):
        self._start_nemesis()

        for node_id, node in enumerate(self.kikimr_cluster.nodes.values()):
            node.ssh_command(
                'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 ; done"',
                raise_on_error=True
            )
        sleep_time_min = 90

        logger.info('Sleeping for {} minute(s)'.format(sleep_time_min))
        time.sleep(sleep_time_min * 60)

        self._stop_nemesis()

    def test_olap_workload(self):
        self._start_nemesis()

        for node_id, node in enumerate(self.kikimr_cluster.nodes.values()):
            node.ssh_command(
                'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/olap_workload --database /Root/db1 ; done"',
                raise_on_error=True
            )
        sleep_time_min = 90

        logger.info('Sleeping for {} minute(s)'.format(sleep_time_min))
        time.sleep(sleep_time_min * 60)

        self._stop_nemesis()

    def test_statistics_workload(self):
        self._start_nemesis()

        log_file = "/Berkanavt/nemesis/log/statistics_workload.log"
        test_path = "/Berkanavt/nemesis/bin/statistics_workload"
        node = list(self.kikimr_cluster.nodes.values())[0]
        node.ssh_command(
            f'screen -d -m bash -c "while true; do sudo {test_path} --database /Root/db1 --log_file {log_file} ; done"',
            raise_on_error=True
        )
        sleep_time_min = 90

        logger.info('Sleeping for {} minute(s)'.format(sleep_time_min))
        time.sleep(sleep_time_min*60)

        self._stop_nemesis()

    @classmethod
    def _start_nemesis(cls):
        for node in cls.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis restart", raise_on_error=True)

    @classmethod
    def _stop_nemesis(cls):
        for node in cls.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis stop", raise_on_error=False)


class TestCheckLivenessAndSafety(object):
    def test_liveness_and_safety(self):
        slice_name = get_slice_name()
        logger.info('slice = {}'.format(slice_name))
        assert slice_name is not None
        kikimr_cluster = ExternalKiKiMRCluster(
            kikimr_configure_binary_path=get_configure_binary_path(),
            config_path=get_slice_directory(),
            kikimr_path=kikimr_driver_path(),
            kikimr_next_path=next_version_kikimr_driver_path(),
            ssh_username=get_ssh_username(),
            deploy_cluster=is_deploy_cluster(),
        )
        composite_assert = CompositeAssert()
        composite_assert.assert_that(
            safety_warden_factory(kikimr_cluster, get_ssh_username()).list_of_safety_violations(),
            is_empty(),
            "No safety violations by Safety Warden"
        )

        composite_assert.assert_that(
            liveness_warden_factory(kikimr_cluster, get_ssh_username()).list_of_liveness_violations,
            is_empty(),
            "No liveness violations by liveness warden",
        )

        composite_assert.finish()
