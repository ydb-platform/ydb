import allure
import logging
import os
import requests
from ydb.tests.olap.lib.utils import get_external_param
import ydb
from copy import deepcopy

LOGGER = logging.getLogger()


class YdbCluster:
    _ydb_driver = None
    _results_driver = None
    _cluster_info = None
    ydb_endpoint = get_external_param('ydb-endpoint', 'grpc://ydb-olap-testing-vla-0002.search.yandex.net:2135')
    ydb_database = get_external_param('ydb-db', 'olap-testing/kikimr/testing/acceptance-2').lstrip('/')
    tables_path = get_external_param('tables-path', 'olap_yatests')
    monitoring_cluster = get_external_param('monitoring_cluster', 'olap_testing_vla')

    @classmethod
    def _get_service_url(cls):
        host = cls.ydb_endpoint.split('://', 2)
        host = host[1 if len(host) > 1 else 0].split('/')[0].split(':')[0]
        port = 8765
        return f'http://{host}:{port}'

    @classmethod
    def _get_cluster_nodes(cls):
        try:
            url = f'{cls._get_service_url()}/viewer/json/nodes'
            headers = {}
            # token = os.getenv('OLAP_YDB_OAUTH', None)
            # if token is not None:
            #    headers['Authorization'] = token
            data = requests.get(url, headers=headers).json()
            nodes = data.get('Nodes', [])
            nodes_count = data.get('TotalNodes', len(nodes))
            return nodes, nodes_count
        except Exception as e:
            LOGGER.error(e)
        return [], 0

    @classmethod
    def get_cluster_info(cls):
        if cls._cluster_info is None:
            version = ''
            cluster_name = ''
            nodes_wilcard = ''
            nodes, node_count = cls._get_cluster_nodes()
            for node in nodes:
                n = node.get('SystemState', {})
                cluster_name = n.get('ClusterName', cluster_name)
                version = n.get('Version', version)
                for tenant in n.get('Tenants', []):
                    if tenant.endswith(cls.ydb_database):
                        nodes_wilcard = n.get('Host', nodes_wilcard).split('.')[0].rstrip('0123456789')
            cls._cluster_info = {
                'database': cls.ydb_database,
                'node_count': node_count,
                'version': version,
                'name': cluster_name,
                'nodes_wilcard': nodes_wilcard,
                'service_url': cls._get_service_url(),
            }
        return deepcopy(cls._cluster_info)

    @staticmethod
    def _create_ydb_driver(endpoint, database, oauth=None, iam_file=None):
        credentials = None
        LOGGER.info(f"Connecting to {endpoint} to {database} ydb_access_token is set {oauth is not None}")

        if oauth is not None:
            credentials = ydb.AccessTokenCredentials(oauth)
        elif iam_file is not None:
            credentials = ydb.iam.ServiceAccountCredentials.from_file(iam_file)

        driver_config = ydb.DriverConfig(
            endpoint,
            database,
            credentials=credentials,
            root_certificates=ydb.load_ydb_root_certificate(),
        )
        driver = ydb.Driver(driver_config)
        try:
            driver.wait(timeout=10)
            LOGGER.info("Connected")
            return driver
        except TimeoutError:
            LOGGER.error(
                f"Connect to YDB failed. \n Last reported errors by discovery: [{driver.discovery_debug_details()}]"
            )
            raise

    @classmethod
    def get_ydb_driver(cls):
        if cls._ydb_driver is None:
            cls._ydb_driver = cls._create_ydb_driver(
                cls.ydb_endpoint, cls.ydb_database, oauth=os.getenv('OLAP_YDB_OAUTH', None)
            )
        return cls._ydb_driver

    @classmethod
    @allure.step('Execute scan query')
    def execute_single_result_query(cls, query, timeout=10):
        allure.attach(query, 'query', attachment_type=allure.attachment_type.TEXT)
        query = ydb.ScanQuery(query, {})
        settings = ydb.BaseRequestSettings()
        settings = settings.with_timeout(timeout)
        try:
            it = cls.get_ydb_driver().table_client.scan_query(query, settings=settings)
            result = next(it)
            return result.result_set.rows[0][0]
        except BaseException:
            LOGGER.error("Cannot connect to YDB")
            raise

    @classmethod
    @allure.step('Check if YDB alive')
    def check_if_ydb_alive(cls, timeout=10):
        try:
            cls.execute_single_result_query("select 1", timeout)
            return True
        except BaseException as ex:
            LOGGER.error(f"Cannot connect to YDB {ex}")
            return False
