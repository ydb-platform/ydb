# -*- coding: utf-8 -*-
import os
import time
import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.common.composite_assert import CompositeAssert # noqa
from ydb.tests.library.harness import param_constants # noqa
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory # noqa
from ydb.tests.library.matchers.collection import is_empty # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa
from ydb.tests.library.common import yatest_common # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


class ImmutableProperty(object):
    def __init__(self, value):
        super(ImmutableProperty, self).__init__()
        self.__value = value

    def __get__(self, instance, owner):
        return self.__value

    def __set__(self, instance, value):
        raise AttributeError('Attribute is immutable')


def read_table_profile():
    with open(yatest_common.source_path('ydb/tests/stability/resources/tbl_profile.txt'), 'r') as reader:
        return reader.read()


def get_slice_directory(slice_name):
    return os.getenv('YDB_CLUSTER_YAML_PATH')


class TestSetupForStability(object):
    stress_binaries_deploy_path = '/Berkanavt/nemesis/bin/'
    artifacts = (
        yatest_common.binary_path('ydb/tests/tools/nemesis/driver/nemesis'),
        yatest_common.binary_path('ydb/tools/simple_queue/simple_queue'),
    )

    @classmethod
    def setup_class(cls):
        cls.slice_name = ImmutableProperty(param_constants.config_name)
        assert cls.slice_name is not None

        logger.info('setup_class started for slice = {}'.format(cls.slice_name))
        cls.kikimr_cluster = kikimr_cluster_factory(config_path=get_slice_directory(cls.slice_name))
        cls._stop_nemesis()
        cls.kikimr_cluster.start()

        if param_constants.deploy_cluster:
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
        slice_name = param_constants.config_name
        logger.info('slice = {}'.format(slice_name))
        assert slice_name is not None
        kikimr_cluster = kikimr_cluster_factory(config_path=get_slice_directory(slice_name))
        composite_assert = CompositeAssert()
        composite_assert.assert_that(
            safety_warden_factory(kikimr_cluster).list_of_safety_violations(),
            is_empty(),
            "No safety violations by Safety Warden"
        )

        composite_assert.assert_that(
            liveness_warden_factory(kikimr_cluster).list_of_liveness_violations,
            is_empty(),
            "No liveness violations by liveness warden",
        )

        composite_assert.finish()
