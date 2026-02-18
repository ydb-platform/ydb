# -*- coding: utf-8 -*-
import sys
import os
import logging
import pytest
import yatest
import time
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

    def _canonize_table_output(self, rows):
        for row in rows:
            if 'Guid' in row and row['Guid'] > 1000:
                row['Guid'] = '<Guid>'
            if 'IcPort' in row:
                row['IcPort'] = '<IcPort>'

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

    def _trace(self, *args, with_grpc_calls=False, with_response=False):
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
            self._canonize_table_output(rows)
            return original_table_dump(table_self, rows, args)

        exit_status = 0
        captured_stdout = StringIO()
        captured_stderr = StringIO()
        with patch.object(common, 'invoke_grpc', side_effect=mock_invoke_grpc), \
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
        return self._canonical_file('results.txt', '\n'.join(results))


class Test(TestBase):
    def test_essential(self):
        retry_assertions(self.check_pdisk_metrics_collected)
        retry_assertions(self.check_vdisks_state_ok)
        return [
            self._trace('--help'),
            self._trace('--unknown-arg'),
            self._trace('device', 'list', '-AH'),
            self._trace('vdisk', 'list', '-AH'),
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

    def test_vdisk_ready_stable_period(self):
        return [
            self._trace('group', 'list'),
            time.sleep(16),  # ReadyStablePeriod + 1 for sure
            self._trace('group', 'list'),
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
