import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class WorkloadTransferBase(WorkloadTestBase):
    workload_binary_name = 'transfer'
    workload_env_var = 'TRANSFER_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    @pytest.mark.parametrize('topic_type', ["local", "remote"],)
    @pytest.mark.parametrize('mode', ["row", "column"],)
    def test_workload_transfer(self, nemesis_enabled: bool, mode: str, topic_type: str):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            f"--database /{YdbCluster.ydb_database} "
            f"--mode {mode} "
            f"--topic {topic_type} "
        )

        additional_stats = {
            "workload_type": "transfer",
            "transfer_template": "workload_transfer_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"TransferWorkload_{mode}_topic_{topic_type}_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestWorkloadTransfer(WorkloadTransferBase):
    timeout = int(get_external_param('workload_duration', 120))
