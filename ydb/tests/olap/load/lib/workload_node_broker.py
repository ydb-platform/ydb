import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class WorkloadNodeBrokerBase(WorkloadTestBase):
    workload_binary_name = 'node_broker'
    workload_env_var = 'NODE_BROKER_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    def test_workload_node_broker(self, nemesis_enabled: bool):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            "--mon-endpoint http://{node_host}:8765 "
            f"--database /{YdbCluster.ydb_database} "
        )

        additional_stats = {
            "workload_type": "node_broker",
            "nodebroker_template": "workload_node_broker_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"NodeBrokerWorkload_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestWorkloadNodeBroker(WorkloadNodeBrokerBase):
    timeout = int(get_external_param('workload_duration', 120))
