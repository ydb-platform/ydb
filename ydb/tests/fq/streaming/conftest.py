import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import ydb
import yatest.common
import pytest

logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint: str, database: str):
        driver_config = ydb.DriverConfig(endpoint, database, auth_token='root@builtin')
        self.driver = ydb.Driver(driver_config)
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def stop(self):
        self.session_pool.stop()
        self.driver.stop()

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
        return self.session_pool.execute_with_retries(statement)

    def query_async(self, statement):
        return self.session_pool.execute_with_retries_async(statement)


@pytest.fixture(scope="function")
def kikimr(request):
    local_checkpoints = request.param["local_checkpoints"]
    
    class Kikimr:
        def __init__(self, client, cluster):
            self.YdbClient = client
            self.Cluster = cluster
        
    def get_ydb_config():
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True
            },
            query_service_config={"available_external_data_sources": ["Ydb"]},
            table_service_config={},
            nodes=2,
            default_clusteradmin="root@builtin"
        )

        config.yaml_config["log_config"]["default_level"] = 8

        query_service_config = config.yaml_config.setdefault("query_service_config", {})
        query_service_config["available_external_data_sources"] = ["ObjectStorage", "Ydb", "YdbTopics"]
        query_service_config["enable_match_recognize"] = True

        #monitoring_config = config.yaml_config.setdefault("monitoring_config", {})
        #monitoring_config["monitoring_port"] = 8765

        database_connection = query_service_config.setdefault("streaming_queries", {}).setdefault("external_storage", {}).setdefault("database_connection", {})
        if not local_checkpoints:
            database_connection["endpoint"] = os.getenv("YDB_ENDPOINT")
            database_connection["database"] = os.getenv("YDB_DATABASE")

        return config

    ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
    logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

    config = get_ydb_config()
    cluster = KiKiMR(config)
    cluster.start()

    node = cluster.nodes[1]
    ydb_client = YdbClient(
        database=f"/{config.domain_name}",
        endpoint=f"grpc://{node.host}:{node.port}"
    )
    ydb_client.wait_connection()

    yield Kikimr(ydb_client, cluster)
    ydb_client.stop()
    cluster.stop()


# @classmethod
#     def get_metrics(cls, metrics: dict[str, dict[str, str]], db_only: bool = False, role: Optional[YdbCluster.Node.Role] = None, counters: str = 'tablets') -> dict[str, dict[str, float]]:
#         def sensor_has_labels(sensor, labels: dict[str, str]) -> bool:
#             for k, v in labels.items():
#                 if sensor.get('labels', {}).get(k, '') != v:
#                     return False
#             return True
#         nodes = cls.get_cluster_nodes(db_only=db_only, role=role)
#         result = {}
#         for node in nodes:
#             url = f'http://{node.host}:{node.mon_port}/counters/'
#             if counters:
#                 url += f'counters={counters}/'
#             url += 'json'
#             response = requests.get(url)
#             response.raise_for_status()
#             sensor_values = {}
#             for name, labels in metrics.items():
#                 for sensor in response.json()['sensors']:
#                     if sensor_has_labels(sensor, labels):
#                         sensor_values.setdefault(name, 0.)
#                         sensor_values[name] += sensor['value']
#             result[node.slot] = sensor_values
#         return result
