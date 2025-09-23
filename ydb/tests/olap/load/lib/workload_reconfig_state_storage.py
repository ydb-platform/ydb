import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class WorkloadReconfigStateStorageBase(WorkloadTestBase):
    workload_binary_name = 'reconfig_state_storage_workload'
    workload_env_var = 'RECONFIG_STATE_STORAGE_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    @pytest.mark.parametrize('config_name', ['StateStorageBoard', 'SchemeBoard', 'StateStorage'],)
    def test_workload_reconfig_state_storage(self, nemesis_enabled: bool, config_name: str):
        command_args_template = (
            f"--database /{YdbCluster.ydb_database} "
            "--grpc_endpoint grpc://{node_host}:2135 "
            "--http_endpoint http://{node_host}:8765 "
            f"--path {config_name}_{{node_host}}_{{iteration_num}} "
            f"--config_name {config_name} "
        )

        additional_stats = {
            "workload_type": "reconfig_state_storage",
            "reconfig_state_storage_template": "workload_reconfig_state_storage_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"ReconfigStateStorageWorkload_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


@pytest.mark.skip(reason="Does not support custom configs")
class TestWorkloadReconfigStateStorage(WorkloadReconfigStateStorageBase):
    timeout = int(get_external_param('workload_duration', 120))
