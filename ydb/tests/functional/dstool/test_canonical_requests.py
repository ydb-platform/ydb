# -*- coding: utf-8 -*-
import sys
import os
import logging
import pytest
import yatest
import yaml

from io import StringIO
from unittest.mock import patch
from google.protobuf import text_format
from ydb.tests.oss.canonical import set_canondata_root

from ydb.apps.dstool.main import main as dstool_main
from ydb.apps.dstool.lib import common
from ydb.apps.dstool.lib import table
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.harness.kikimr_cluster import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.core.protos.whiteboard_disk_states_pb2 import EVDiskState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig
from .conftest import BaseConfigBuilder

logger = logging.getLogger(__name__)
C_4GB = 4 * 2**30

# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
NODES_COUNT = 8
CLUSTER_CONFIG = dict(
    erasure=Erasure.BLOCK_4_2,
    nodes=NODES_COUNT,
    dynamic_pdisks=[{'user_kind': 0}],
    dynamic_pdisk_size=C_4GB,
    static_pdisk_config={'expected_slot_count': 9},
    dynamic_pdisks_config={'expected_slot_count': 9},
    dynamic_storage_pools=[dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0, num_groups=1)],
    additional_log_configs={
        'BS_NODE': LogLevels.DEBUG,
        'BS_CONTROLLER': LogLevels.DEBUG,
    },
)


@pytest.fixture(scope='function')
def ydb_cluster(ydb_configurator, request):
    """Override ydb_cluster fixture to use function scope for test isolation."""
    test_name = request.node.name

    logger.info("Setting up ydb_cluster for test %s", test_name)
    cluster = KiKiMR(
        configurator=ydb_configurator,
    )
    cluster.is_local_test = True
    cluster.start(timeout_seconds=20)

    yield cluster

    logger.info("Tearing down ydb_cluster for test %s", test_name)
    cluster.stop()


class TestBase:
    @pytest.fixture(autouse=True)
    def setup(self, ydb_cluster):
        set_canondata_root('ydb/tests/functional/dstool/canondata')
        self.cluster = ydb_cluster
        self.host = ydb_cluster.nodes[1].host
        self.grpc_port = ydb_cluster.nodes[1].grpc_port
        self.mon_port = ydb_cluster.nodes[1].mon_port
        self.endpoint = 'grpc://%s:%s' % (self.host, self.grpc_port)

    def _canonical_file(self, filename, content):
        path = os.path.join(yatest.common.output_path(), filename)
        with open(path, 'w') as w:
            w.write(content)
        return yatest.common.canonical_file(path, local=True, universal_lines=True)

    def _canonize_request(self, request):
        for command in request.Command:
            if command.HasField('UpdateDriveStatus'):
                cmd = command.UpdateDriveStatus
                if cmd.HasField('HostKey'):
                    cmd.HostKey.IcPort = 999999

    def _canonize_table_output(self, rows, canonize_columns=None):
        for row in rows:
            if 'Guid' in row and row['Guid'] > 1000:
                row['Guid'] = '<Guid>'
            if 'IcPort' in row:
                row['IcPort'] = '<IcPort>'
            if canonize_columns:
                for column in canonize_columns:
                    if column in row:
                        row[column] = f'<{column}>'

    def check_pdisk_metrics_collected(self):
        base_config = self.cluster.client.query_base_config().BaseConfig
        assert len(base_config.PDisk) == 2 * NODES_COUNT
        for pdisk in base_config.PDisk:
            assert pdisk.PDiskMetrics.HasField('UpdateTimestamp')
        return base_config

    def check_vdisks_state_ok(self):
        base_config = self.cluster.client.query_base_config().BaseConfig
        assert len(base_config.VSlot) == 16  # 1 static + 1 dynamic group
        for vslot in base_config.VSlot:
            assert vslot.VDiskMetrics.State == EVDiskState.OK

    def _trace(self, *args, with_grpc_calls=False, with_response=False, canonize_columns=None,
               mock_base_config=None):
        common.cache.clear()
        common.name_cache.clear()
        results = []
        results.append(' '.join(['dstool -e <endpoint> --mon-port <mon_port>', *args]))
        args = ['-e', self.endpoint, '--mon-port', self.mon_port, *args]

        grpc_calls = []
        original_invoke_grpc = common.invoke_grpc

        def mock_invoke_grpc(func, *params, **kwargs):
            response = original_invoke_grpc(func, *params, **kwargs)
            grpc_calls.append(f'=== Invoke {func} ===')
            for param in params:
                self._canonize_request(param.Request)
                grpc_calls.append(text_format.MessageToString(param, as_one_line=False))

            if with_response:
                grpc_calls.append('--- Response ---')
                grpc_calls.append(text_format.MessageToString(response, as_one_line=False))
            return response

        def mock_exit(code=0):
            raise SystemExit(code)

        def mock_get_terminal_size():
            return os.terminal_size((80, 24))

        original_table_dump = table.TableOutput.dump

        def mock_table_dump(table_self, rows, args):
            self._canonize_table_output(rows, canonize_columns=canonize_columns)
            return original_table_dump(table_self, rows, args)

        if mock_base_config is not None:
            invoke_grpc_patch = patch.object(common, 'fetch_base_config_and_storage_pools', return_value=mock_base_config)
        else:
            invoke_grpc_patch = patch.object(common, 'invoke_grpc', side_effect=mock_invoke_grpc)

        exit_status = 0
        captured_stdout = StringIO()
        captured_stderr = StringIO()
        with invoke_grpc_patch, \
             patch.object(sys, 'exit', side_effect=mock_exit), \
             patch.object(sys, 'argv', ['dstool']), \
             patch('sys.stdout', captured_stdout), \
             patch('sys.stderr', captured_stderr), \
             patch('shutil.get_terminal_size', side_effect=mock_get_terminal_size), \
             patch.object(table.TableOutput, 'dump', mock_table_dump):
            try:
                dstool_main(args)
            except SystemExit as e:
                exit_status = e.code

        results.append(f'Exit Status: {exit_status}')

        captured_stdout = captured_stdout.getvalue()
        captured_stderr = captured_stderr.getvalue()
        if captured_stdout:
            results.extend(['===== stdout =====', captured_stdout])
        if captured_stderr:
            results.extend(['===== stderr =====', captured_stderr])
        if with_grpc_calls:
            results.extend(['===== grpc_calls =====', '\n'.join(grpc_calls)])
        if mock_base_config is not None:
            results.extend([
                '===== mock_base_config =====',
                text_format.MessageToString(mock_base_config['BaseConfig'], as_one_line=False),
            ])
            for sp in mock_base_config['StoragePools']:
                results.extend([
                    '===== DefineStoragePool =====',
                    text_format.MessageToString(sp, as_one_line=False),
                ])
        return self._canonical_file('results.txt', '\n'.join(results))


class Test(TestBase):
    def test_essential(self):
        retry_assertions(self.check_pdisk_metrics_collected)
        retry_assertions(self.check_vdisks_state_ok)
        return [
            self._trace('--help'),
            self._trace('--unknown-arg'),
            self._trace('device', 'list', '-AH'),
            self._trace('vdisk', 'list', '-AH', canonize_columns=['NodeId:PDiskId', 'NodeId']),
            self._trace('group', 'list', '-AH'),
            self._trace('pdisk', 'list', '-AH'),
            self._trace('pool', 'list', '-AH'),
            self._trace('box', 'list', '-AH'),
            self._trace('node', 'list', '-A'),
            self._trace('cluster', 'list', '-A'),
        ]

    def test_pdisk_set_status_inactive(self):
        return [
            self._trace('--dry-run', 'pdisk', 'set', '--status=INACTIVE', '--pdisk-ids', '[1:1]', with_grpc_calls=True),
            self._trace('pdisk', 'set', '--status=INACTIVE', '--pdisk-ids', '[1:1000]', '[2:1]', with_grpc_calls=True),
            self._trace('pdisk', 'list', '--columns', 'NodeId:PDiskId', 'Status', with_grpc_calls=True),
        ]

    def test_cluster_get_set(self):
        return [
            self._trace('cluster', 'get', with_grpc_calls=True),
            self._trace('cluster', 'get', '-A', '--format=json', with_grpc_calls=True),
            self._trace('cluster', 'set', '--default-max-slots=4', with_grpc_calls=True),
            self._trace('cluster', 'set', '--disable-self-heal', with_grpc_calls=True),
            self._trace('cluster', 'set', '--disable-donor-mode', with_grpc_calls=True),
            self._trace('cluster', 'set', '--scrub-periodicity', '1d1h1m1s', with_grpc_calls=True),
            self._trace('cluster', 'set', '--scrub-periodicity', 'disable', with_grpc_calls=True),
            self._trace('cluster', 'set', '--pdisk-space-margin-promille', '1000', with_grpc_calls=True),
            self._trace('cluster', 'set', '--pdisk-space-color-border', 'LIGHT_YELLOW', with_grpc_calls=True),
            self._trace('cluster', 'get', '-A', '--format=json', with_grpc_calls=True),

            # Errors:
            self._trace('cluster', 'set', '--enable-self-heal', '--enable-donor-mode'),
            self._trace('cluster', 'set', '--pdisk-space-margin-promille', '1001'),
            self._trace('cluster', 'set', '--pdisk-space-color-border', 'UNKNOWN'),
            self._trace('--dry-run', 'cluster', 'set', '--disable-self-heal'),
        ]

    def test_group_take_snapshot(self):
        retry_assertions(self.check_vdisks_state_ok)

        def check_vdisk_state_error():
            base_config = self.cluster.client.query_base_config().BaseConfig
            vslots = [vslot for vslot in base_config.VSlot if vslot.VSlotId.NodeId == 1 and vslot.VSlotId.PDiskId == 1]
            assert len(vslots) == 1
            vslot = vslots[0]
            assert vslot.GroupId == 0
            assert vslot.VDiskMetrics.State == EVDiskState.PDiskError

        return [
            self._trace('group', 'take-snapshot', '--group-ids=0', '--output=group0_1.bin'),
            self._trace('pdisk', 'stop', '--node-id=1', '--pdisk-id=1'),
            retry_assertions(check_vdisk_state_error, timeout_seconds=20),
            self._trace('group', 'take-snapshot', '--group-ids=0', '--output=group0_2.bin'),
        ]

    def test_capacity_metrics(self):
        retry_assertions(self.check_pdisk_metrics_collected)
        retry_assertions(self.check_vdisks_state_ok)

        vdisk_columns = [
            'VDiskId',
            'NodeId:PDiskId',
            'VDiskSlotUsage',
            'VDiskRawUsage',
            'NormalizedOccupancy',
            'UsedSize',
            'SlotSize',
            'TotalSize',
            'CapacityAlert',
            'GroupSizeInUnits',
        ]
        group_columns = [
            'GroupId',
            'SizeInUnits',
            'VDiskSlotUsage',
            'VDiskRawUsage',
            'NormalizedOccupancy',
            'UsedSize',
            'Limit',
            'TotalSize',
            'CapacityAlert',
        ]

        return [
            self._trace('vdisk', 'list', '-H', '--columns', *vdisk_columns),
            self._trace('group', 'list', '-H', '--columns', *group_columns),
            self._trace('pool', 'list', '-H', '--show-vdisk-estimated-usage'),
        ]

    def test_group_resize(self):
        group_id = 2181038080
        return [
            self._trace('group', 'resize', '--size-in-units', '2', '--group-ids', str(group_id), with_grpc_calls=True),
            self._trace('vdisk', 'list', '-H', '--columns', 'VDiskId', 'GroupSizeInUnits'),
            self._trace('group', 'list', '-H', '--columns', 'GroupId', 'SizeInUnits'),
            # Errors:
            self._trace('group', 'resize', '--size-in-units', '1', '--group-ids', str(group_id+1), '--format', 'json'),
        ]

    def test_infer_pdisk_slot_count(self):
        dynconfig_client = DynConfigClient(self.host, self.grpc_port)

        def generate_config():
            generate_config_response = dynconfig_client.fetch_startup_config()
            logger.info(f"{generate_config_response=}")
            assert generate_config_response.operation.status == StatusIds.SUCCESS

            result = dynconfig.FetchStartupConfigResult()
            generate_config_response.operation.result.Unpack(result)

            return {
                "metadata": {
                    "kind": "MainConfig",
                    "version": 0,
                    "cluster": "",
                },
                "config": yaml.safe_load(result.config)
            }

        def replace_config(full_config):
            replace_config_response = dynconfig_client.replace_config(yaml.dump(full_config))
            logger.info(f"{replace_config_response=}")
            assert replace_config_response.operation.status == StatusIds.SUCCESS

        full_config = generate_config()
        full_config["config"]["blob_storage_config"]["infer_pdisk_slot_count_settings"] = {
            "rot": {
                "prefer_inferred_settings_over_explicit": True,
                "unit_size": C_4GB,
                "max_slots": 12,
            }
        }
        replace_config(full_config)

        def check_pdisk_metrics_updated():
            base_config = self.check_pdisk_metrics_collected()
            for pdisk in base_config.PDisk:
                assert pdisk.PDiskMetrics.SlotSizeInUnits > 0
        retry_assertions(check_pdisk_metrics_updated)

        pdisk_columns = [
            'NodeId:PDiskId',
            'Path',
            'TotalSize',
            'ExpectedSlotCount',
            'SlotSizeInUnits',
        ]

        trace1 = self._trace('pdisk', 'list', '-H', '--columns', *pdisk_columns)

        del full_config["config"]["blob_storage_config"]["infer_pdisk_slot_count_settings"]
        full_config["metadata"]["version"] = 1
        replace_config(full_config)

        def check_pdisk_metrics_updated():
            base_config = self.check_pdisk_metrics_collected()
            for pdisk in base_config.PDisk:
                assert pdisk.PDiskMetrics.SlotSizeInUnits == 0
        retry_assertions(check_pdisk_metrics_updated)

        trace2 = self._trace('pdisk', 'list', '-H', '--columns', *pdisk_columns)

        return [trace1, trace2]

    def test_pdisk_check_leaked_slots(self):
        retry_assertions(self.check_pdisk_metrics_collected)

        # Initialize dstool connection params to allow common.fetch_json_info
        import argparse
        p = argparse.ArgumentParser()
        common.add_host_access_options(p)
        common.apply_args(p.parse_args(['-e', self.endpoint, '--mon-port', str(self.mon_port), '-q']))

        base_config = self.cluster.client.query_base_config().BaseConfig
        pdisk_node_ids = sorted({pdisk.NodeId for pdisk in base_config.PDisk})
        expected_pdisk_ids = {(pdisk.NodeId, pdisk.PDiskId) for pdisk in base_config.PDisk}

        def check_whiteboard_pdisk_info_available():
            pdisk_whiteboard_info = common.fetch_json_info('pdiskinfo', nodes=pdisk_node_ids)
            for pdisk_id in expected_pdisk_ids:
                wb_info = pdisk_whiteboard_info.get(pdisk_id, {})
                assert 'NumActiveSlots' in wb_info

        retry_assertions(check_whiteboard_pdisk_info_available)

        vdisk_evict_cmd = ['vdisk', 'evict', '--ignore-degraded-group-check', '--ignore-failure-model-group-check']
        return [
            self._trace(*vdisk_evict_cmd, '--vdisk-ids', '[82000000:_:0:0:0]', with_grpc_calls=True),
            self._trace(*vdisk_evict_cmd, '--vdisk-ids', '[82000000:_:0:1:0]', '--suppress-donor-mode', with_grpc_calls=True),
            self._trace('--quiet', 'pdisk', 'list', '--check-leaked-slots'),
        ]

    def test_pool_estimated_usage(self):
        builder = (
            BaseConfigBuilder()
            .add_node(node_id=1)
            .add_pdisk(node_id=1, pdisk_id=1001, expected_slot_count=16, enforced_dynamic_slot_size=int(200e9))  # 3.2 TB
            .add_group(group_id=0x80000001, vslot_ids=[(1, 1001, 1000)])
            .add_vslot(
                node_id=1, pdisk_id=1001, vslot_id=1000, group_id=0x80000001,
                allocated_size=int(40e9),
                available_size=0,
            )
            .add_storage_pool(name='test-pool', erasure_species='none', kind='hdd')
        )

        def _trace_pool_list():
            return self._trace('pool', 'list', '-H', '--format=json', '--show-vdisk-estimated-usage', mock_base_config=builder.build())

        return [
            _trace_pool_list(),

            # Replace pdisk 3.2 -> 6.4 TB and markup SlotSizeInUnits = 2
            builder.update_pdisk(node_id=1, pdisk_id=1001, slot_size_in_units=2, enforced_dynamic_slot_size=int(400e9)) and None,
            builder.update_vslot(node_id=1, pdisk_id=1001, vslot_id=1000, available_size=int(360e9)) and None,
            _trace_pool_list(),

            # Change GroupSizeInUnits = 4
            builder.update_group(group_id=0x80000001, group_size_in_units=4) and None,
            _trace_pool_list(),
        ]
