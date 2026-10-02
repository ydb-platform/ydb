import pytest
from ydb.tests.olap.load.lib.workload_executor_parallel import ParallelWorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    'nemesis_enabled', [True, False],
    ids=['nemesis_true', 'nemesis_false']
)
class TestWorkloadParallel(ParallelWorkloadTestBase):
    timeout = int(get_external_param('workload_duration', 120))

    def test_all_workloads_parallel(self, nemesis_enabled: bool):

        runnable_workloads = {
            'workload_cdc': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}"],
                'local_path': 'ydb/tests/stress/cdc/cdc'
            },
            'workload_ctas': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--path", "workload_ctas_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/ctas/ctas'
            },
            'workload_kafka': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--bootstrap", "http://{node_host}:11223",
                         "--source-path", "workload_source_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                         "--target-path", "workload_target_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                         "--consumer", "workload-consumer-{iteration_num}-{uuid}",
                         "--num-workers", "2"],
                'local_path': 'ydb/tests/stress/kafka/kafka_streams_test'
            },
            'workload_kv': {
                'args': ["--endpoint", "grpc://{node_host}:2135", "--database", f"/{YdbCluster.ydb_database}",
                         "--store_type", "row", "--kv_prefix", "workload_kv_{node_host}_iter_{iteration_num}_{uuid}", f"--duration={self.timeout}"],
                'local_path': 'ydb/tests/stress/kv/workload_kv'
            },
            'workload_log': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--store_type", "row",
                         "--log_prefix", "log_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/log/workload_log'
            },
            'workload_mixed': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--store_type", "row",
                         "--mixed_prefix", "mixed_mixed_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/mixedpy/workload_mixed'
            },
            'workload_node_broker': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--mon-endpoint", "http://{node_host}:8765",
                         "--database", f"/{YdbCluster.ydb_database}"],
                'local_path': 'ydb/tests/stress/node_broker/node_broker'
            },
            'workload_olap': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--path", "olap_workload_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/olap_workload/olap_workload'
            },
            'workload_oltp': {
                'args': ["--endpoint", "grpc://{node_host}:2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--path", "oltp_workload_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/oltp_workload/oltp_workload'
            },
            # TODO does not support custom cluster configs
            # 'workload_reconfig_state_storage': {
            #     'args': ,
            #     'local_path': 'ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload'
            # },
            'workload_show_create': {
                'args': ["--endpoint", "grpc://{node_host}:2135", "--database", f"/{YdbCluster.ydb_database}",
                         "--path-prefix", "workload_show_create_{node_host}_iter_{iteration_num}_{uuid}", f"--duration={self.timeout}"],
                'local_path': 'ydb/tests/stress/show_create/view/show_create_view'
            },
            'workload_simple_queue': {
                'args': ["--endpoint", f"{YdbCluster.ydb_endpoint}",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--mode", "row"],
                'local_path': 'ydb/tests/stress/simple_queue/simple_queue'
            },
            'workload_statistics': {
                'args': ["--host", "{node_host}",
                         "--port", "2135",
                         "--database", f"/{YdbCluster.ydb_database}",
                         "--prefix", "statistics_workload_{node_host}_iter_{iteration_num}_{uuid}"],
                'local_path': 'ydb/tests/stress/statistics_workload/statistics_workload'
            },
            'workload_topic_kafka': {
                'args': [
                    "--endpoint", "grpc://{node_host}:2135",
                    "--database", f"/{YdbCluster.ydb_database}",
                    "--topic_prefix", "workload_source_topic_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                    "--duration", "120",
                    "--consumers", "2",
                    "--consumer-threads", "2",
                    "--restart-interval", "15s",
                    "--partitions", "4",
                    "--write-workload", "0.01", "9000000", "2", "big_record", "1",
                    "--write-workload", "8000", "45", "1000", "small_record", "10",
                    "--write-workload", "800", "4096", "1", "medium_record", "10",
                ],
                'local_path': 'ydb/tests/stress/topic_kafka/workload_topic_kafka'
            },
            'workload_topic': {
                'args': [
                    "--endpoint", "grpc://{node_host}:2135",
                    "--database", f"/{YdbCluster.ydb_database}",
                    "--topic_prefix", "workload_topic_{node_host}_iter_{iteration_num}_{uuid}",
                ],
                'local_path': 'ydb/tests/stress/topic/workload_topic'
            },
            'workload_transfer': {
                'args': [
                    "--endpoint", "grpc://{node_host}:2135",
                    "--database", f"/{YdbCluster.ydb_database}",
                    "--mode", "row",
                    "--topic", "local",
                ],
                'local_path': 'ydb/tests/stress/transfer/transfer'
            },
        }

        self.execute_parallel_workloads_test(
            workload_params=runnable_workloads,
            duration_value=self.timeout,
            nemesis_enabled=nemesis_enabled,
            nodes_percentage=100
        )
