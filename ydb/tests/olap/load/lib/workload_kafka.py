import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class WorkloadKafkaBase(WorkloadTestBase):
    workload_binary_name = 'kafka_streams_test'
    workload_env_var = 'KAFKA_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    def test_workload_kafka(self, nemesis_enabled: bool):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            f"--database /{YdbCluster.ydb_database} "
            "--bootstrap http://{node_host}:11223 "
            "--source-path workload_source_kafka_{node_host}_iter_{iteration_num}_{uuid} "
            "--target-path workload_target_kafka_{node_host}_iter_{iteration_num}_{uuid} "
            "--consumer workload-consumer-{iteration_num}-{uuid} "
            "--num-workers 2 "
        )

        additional_stats = {
            "workload_type": "kafka",
            "kafka_template": "workload_kafka_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"KafkaWorkload_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestWorkloadKafka(WorkloadKafkaBase):
    timeout = int(get_external_param('workload_duration', 120))
