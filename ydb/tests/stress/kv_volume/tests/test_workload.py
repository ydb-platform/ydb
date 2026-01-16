# -*- coding: utf-8 -*-
import pytest
import sys

import ydb.tests.stress.kv_volume.protos.config_pb2 as config_pb

from ydb.tests.stress.kv_volume.workload import YdbKeyValueVolumeWorkload as Workload, ConfigBuilder
from ydb.tests.library.stress.fixtures import StressFixture


def generate_configs_for_tests():
    return [
        (  # common channel read
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_initial_data()
            .add_write_to_initial_data(size=4096, count=5, channel=0)
            .init_prepared_action(name='read')
            .add_read_to_prepared_action(size=1024, count=5)
            .set_periodicity_for_prepared_action(period_us=10000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .return_config()
        ),
        (  # inline channel read
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_initial_data()
            .add_write_to_initial_data(size=4096, count=5, channel=1)
            .init_prepared_action(name='read')
            .add_read_to_prepared_action(size=1024, count=5)
            .set_periodicity_for_prepared_action(period_us=10000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .return_config()
        ),
        (  # write then delete from prev actions
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_prepared_action(name='write')
            .add_write_to_prepared_action(size=4096, count=5, channel=0)
            .set_periodicity_for_prepared_action(period_us=50000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .init_prepared_action(name='read', parent_action='write')
            .add_read_to_prepared_action(size=1024, count=5)
            .set_data_mode_from_prev_actions_for_prepared_action(['write'])
            .add_prepared_action()
            .init_prepared_action(name='delete', parent_action='read')
            .add_delete_to_prepared_action(count=5)
            .set_data_mode_from_prev_actions_for_prepared_action(['write'])
            .add_prepared_action()
            .return_config()
        ),
    ]


class TestYdbKvVolumeWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        print('setup cluster', file=sys.stderr)
        yield from self.setup_cluster()

    @pytest.mark.parametrize("config", generate_configs_for_tests())
    @pytest.mark.parametrize("version", ["v1", "v2"])
    def test(self, config, version):
        print('Test begin', file=sys.stderr)
        workload = Workload(self.endpoint, self.database, 1, 1, version, config, verbose=False)
        print('Workload begin', file=sys.stderr)
        workload.start()
        workload.wait_stop()
        print('Test end', file=sys.stderr)
