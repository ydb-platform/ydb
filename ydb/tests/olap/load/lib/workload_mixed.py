import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
MIXEDGER = logging.getLogger(__name__)


class WorkloadMixedBase(WorkloadTestBase):
    workload_binary_name = 'workload_mixed'
    workload_env_var = 'MIXED_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    @pytest.mark.parametrize('store_type', ['row', 'column'])
    def test_workload_mixed(self, nemesis_enabled: bool, store_type: str):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            f"--database /{YdbCluster.ydb_database} "
            f"--store_type {store_type} "
            "--mixed_prefix mixed_mixed_{node_host}_iter_{iteration_num}_{uuid} "
        )

        additional_stats = {
            "workload_type": "mixed",
            "mixed_template": "workload_mixed_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"MixedWorkload_{store_type}_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestWorkloadMixed(WorkloadMixedBase):
    timeout = int(get_external_param('workload_duration', 120))
