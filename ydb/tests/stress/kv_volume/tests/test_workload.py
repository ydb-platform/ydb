# -*- coding: utf-8 -*-
import pytest
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
    def add_read_to_prepared_action(self, size, count):
        command = config_pb.ActionCommand()
        command.read.size = size 
        command.read.count = count
        self.prepared_action.action_command.append(command)

    @chained_method
    def add_write_to_prepared_action(self, size, count, channel):
        command = config_pb.ActionCommand()
        command.write.size = size
        command.write.count = count
        command.write.channel = channel
        self.prepared_action.action_command.append(command)

    @chained_method
    def set_data_mode_for_prepared_action(self, worker=False, from_prev_actions=[]):
        if worker:
            self.prepared_action.action_data_mode.worker
        else:
            mode = config_pb.ActionDataMode.FromPrevActions(action_name=from_prev_actions)
            self.prepared_action.action_data_mode = config_pb.ActionDataMode(from_prev_actions=mode)

    @chained_method
    def set_periodicity_for_prepared_action(self, period_us=None, worker_max_in_flight=None, global_max_in_flight=None):
        if period_us is not None:
            self.prepared_action.period_us = period_us
        if worker_max_in_flight is not None:
            self.prepared_action.worker_max_in_flight = worker_max_in_flight
        if global_max_in_flight is not None:
            self.prepared_action.global_max_in_flight = global_max_in_flight

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
            .set_data_mode_for_prepared_action(worker=True)
            .return_config()
        )
    ]


class TestYdbKvVolumeWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @pytest.mark.parametrize("config", generate_configs_for_tests())
    @pytest.mark.parametrize("version", ["v1", "v2"])
    def test(self, config, version):
        with Workload(self.endpoint, self.database, 60, "read_inline", 1, version, config) as workload:
            workload.start()
