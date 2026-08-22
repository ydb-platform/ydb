import pytest
import time
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    'nemesis_enabled', [True, False],
    ids=['nemesis_true', 'nemesis_false']
)
class OlapWorkloadBase(WorkloadTestBase):
    # Настройки для базового класса
    workload_binary_name = 'olap_workload'
    workload_env_var = 'OLAP_WORKLOAD_BINARY'

    def test_workload_olap(self, nemesis_enabled: bool):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            f"--database /{YdbCluster.ydb_database} "
            "--path olap_workload_{node_host}_iter_{iteration_num}_{uuid}"
        )

        additional_stats = {
            "workload_type": "olap",
            "path_template": "olap_workload_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"OlapWorkload_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestOlapWorkload(OlapWorkloadBase):
    """Тест OLAP workload с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
