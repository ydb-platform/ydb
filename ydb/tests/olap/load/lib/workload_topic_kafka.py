import time
import pytest
from .workload_executor import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class WorkloadTopicKafkaBase(WorkloadTestBase):
    workload_binary_name = 'workload_topic_kafka'
    workload_env_var = 'TOPIC_KAFKA_WORKLOAD_BINARY'

    @pytest.mark.parametrize(
        'nemesis_enabled', [True, False],
        ids=['nemesis_true', 'nemesis_false']
    )
    def test_workload_topic_kafka(self, nemesis_enabled: bool):
        command_args_template = (
            "--endpoint grpc://{node_host}:2135 "
            f"--database /{YdbCluster.ydb_database} "
            "--topic_prefix workload_source_topic_kafka_{node_host}_iter_{iteration_num}_{uuid} "
            "--duration 120 "
            "--consumers 2 "
            "--consumer-threads 2 "
            "--restart-interval 15s "
            "--partitions 4 "
            "--write-workload 0.01 9000000 2 big_record 1 "
            "--write-workload 8000 45 1000 small_record 10 "
            "--write-workload 800 4096 1 medium_record 10 "
        )

        additional_stats = {
            "workload_type": "topic_kafka",
            "topic_kafka_template": "workload_topic_kafka_{node_host}_iter_{iteration_num}_{uuid}",
            "nemesis": nemesis_enabled,
            "test_timestamp": int(time.time()),
        }

        self.execute_workload_test(
            workload_name=f"TopicKafkaWorkload_nemesis_{nemesis_enabled}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration",
            nemesis=nemesis_enabled,
            nodes_percentage=100
        )


class TestWorkloadTopicKafka(WorkloadTopicKafkaBase):
    timeout = int(get_external_param('workload_duration', 120))
