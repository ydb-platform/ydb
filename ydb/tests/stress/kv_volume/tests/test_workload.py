# -*- coding: utf-8 -*-
import pytest
import sys
from functools import wraps

import ydb.tests.stress.kv_volume.protos.config_pb2 as config_pb

from ydb.tests.stress.kv_volume.workload import YdbKeyValueVolumeWorkload as Workload
from ydb.tests.library.stress.fixtures import StressFixture


def chained_method(method):
    @wraps(method)
    def _wrapped(self, *args, **kwargs):
        method(self, *args, **kwargs)
        return self

    return _wrapped


class ConfigBuilder:
    def __init__(self, partition_mode):
        self.config = config_pb.KeyValueVolumeStressLoad()
        self.config.partition_mode = partition_mode
        self.prepared_action = None

    @chained_method
    def set_volume_config(self, path, partition_count, channel_medias):
        self.config.volume_config.path = path
        self.config.volume_config.partition_count = partition_count
        self.config.volume_config.channel_media.extend(channel_medias)
        return self

    @chained_method
    def init_prepared_action(self, name, parent_action=None):
        self.prepared_action = config_pb.Action()
        self.prepared_action.name = name
        if parent_action is not None:
            self.prepared_action.parent_action = parent_action

    @chained_method
    def add_prepared_action(self):
        self.config.actions.append(self.prepared_action)
        self.drop_prepared_action()

    @chained_method
    def drop_prepared_action(self):
        self.prepared_action = None

    @chained_method
    def add_print_to_prepared_action(self, msg):
        command = config_pb.ActionCommand()
        command.print.msg = msg
        self.prepared_action.action_command.append(command)

    @chained_method
    def add_read_to_prepared_action(self, size, count, verify_data=False):
        command = config_pb.ActionCommand()
        command.read.size = size
        command.read.count = count
        command.read.verify_data = True
        self.prepared_action.action_command.append(command)

    @chained_method
    def add_write_to_prepared_action(self, size, count, channel):
        command = config_pb.ActionCommand()
        command.write.size = size
        command.write.count = count
        command.write.channel = channel
        self.prepared_action.action_command.append(command)

    @chained_method
    def add_delete_to_prepared_action(self, count):
        command = config_pb.ActionCommand()
        command.delete.count = count
        self.prepared_action.action_command.append(command)

    @chained_method
    def set_data_mode_worker_for_prepared_action(self):
        self.prepared_action.action_data_mode.worker.CopyFrom(config_pb.ActionDataMode.Worker())

    @chained_method
    def set_data_mode_from_prev_actions_for_prepared_action(self, action_names):
        mode = config_pb.ActionDataMode.FromPrevActions(action_name=action_names)
        self.prepared_action.action_data_mode.from_prev_actions.CopyFrom(mode)

    @chained_method
    def set_periodicity_for_prepared_action(self, period_us=None, worker_max_in_flight=None, global_max_in_flight=None):
        if period_us is not None:
            self.prepared_action.period_us = period_us
        if worker_max_in_flight is not None:
            self.prepared_action.worker_max_in_flight = worker_max_in_flight
        if global_max_in_flight is not None:
            self.prepared_action.global_max_in_flight = global_max_in_flight

    @chained_method
    def init_initial_data(self):
        if not self.config.HasField('initial_data'):
            self.config.initial_data.Clear()

    @chained_method
    def clear_initial_data(self):
        self.config.initial_data.Clear()

    @chained_method
    def add_write_to_initial_data(self, size, count, channel):
        write_cmd = config_pb.ActionCommand.Write()
        write_cmd.size = size
        write_cmd.count = count
        write_cmd.channel = channel
        self.config.initial_data.write_commands.append(write_cmd)

    def return_config(self):
        return self.config


def generate_configs_for_tests():
    return [
        (
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_prepared_action(name='print')
            .add_print_to_prepared_action('test msg')
            .set_periodicity_for_prepared_action(period_us=1000000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .return_config()
        ),
        (  # common channel read
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_initial_data()
            .add_write_to_initial_data(size=1024*1024, count=5, channel=0)
            .init_prepared_action(name='read')
            .add_read_to_prepared_action(size=1024, count=5, verify_data=True)
            .set_periodicity_for_prepared_action(period_us=10000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .return_config()
        ),
        (  # inline channel read
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_initial_data()
            .add_write_to_initial_data(size=1024*1024, count=5, channel=1)
            .init_prepared_action(name='read')
            .add_read_to_prepared_action(size=1024, count=5, verify_data=True)
            .set_periodicity_for_prepared_action(period_us=10000)
            .set_data_mode_worker_for_prepared_action()
            .add_prepared_action()
            .return_config()
        ),
        (  # delete action
            ConfigBuilder(config_pb.PartitionMode.OnePartition)
            .set_volume_config("kv_volume", 1, ['ssd'] * 3)
            .init_initial_data()
            .add_write_to_initial_data(size=1024, count=10, channel=0)
            .init_prepared_action(name='delete')
            .add_delete_to_prepared_action(count=2)
            .set_periodicity_for_prepared_action(period_us=1000000)
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
            .add_read_to_prepared_action(size=1024, count=5, verify_data=True)
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
        workload = Workload(self.endpoint, self.database, 10, 1, version, config, verbose=False)
        print('Workload begin', file=sys.stderr)
        workload.start()
        workload.wait_stop()
        print('Test end', file=sys.stderr)
