from __future__ import annotations
import allure
import logging
import os
import requests
from ydb.tests.olap.lib.utils import get_external_param
import ydb
from copy import deepcopy
from time import sleep, time
from typing import List

LOGGER = logging.getLogger()


class YdbCluster:
    class MonitoringUrl:
        def __init__(self, url: str, caption: str = 'link') -> None:
            self.url = url
            if self.url.find('://') < 0:
                self.url = f'https://{self.url}'
            self.caption = caption

    _ydb_driver = None
    _results_driver = None
    _cluster_info = None
    ydb_endpoint = get_external_param('ydb-endpoint', 'grpc://ydb-olap-testing-vla-0002.search.yandex.net:2135')
    ydb_database = get_external_param('ydb-db', 'olap-testing/kikimr/testing/acceptance-2').lstrip('/')
    tables_path = get_external_param('tables-path', 'olap_yatests')
    _monitoring_urls: list[YdbCluster.MonitoringUrl] = None

    @classmethod
    def get_monitoring_urls(cls) -> list[YdbCluster.MonitoringUrl]:
        def _process_url(url: str) -> YdbCluster.MonitoringUrl:
            spl = url.split('::', 2)
            if len(spl) == 1:
                return YdbCluster.MonitoringUrl(spl[0])
            return YdbCluster.MonitoringUrl(spl[1], spl[0])
        if cls._monitoring_urls is None:
            cls._monitoring_urls = [
                _process_url(url)
                for url in get_external_param('monitoring_urls', (
                    'monitoring.yandex-team.ru/projects/kikimr/dashboards/mone0310v4dbc6kui89v?'
                    'p.cluster=olap_testing_vla&p.database=/{database}&from={start_time}&to={end_time}')
                    ).split(',')
            ]
        return cls._monitoring_urls

    @classmethod
    def _get_service_url(cls):
        host = cls.ydb_endpoint.split('://', 2)
        host = host[1 if len(host) > 1 else 0].split('/')[0].split(':')[0]
        port = 8765
        return f'http://{host}:{port}'

    @classmethod
    def get_cluster_nodes(cls, path=None):
        try:
            url = f'{cls._get_service_url()}/viewer/json/nodes?database=/{cls.ydb_database}'
            if path is not None:
                url += f'&path={path}&tablets=true'
            headers = {}
            # token = os.getenv('OLAP_YDB_OAUTH', None)
            # if token is not None:
            #    headers['Authorization'] = token
            data = requests.get(url, headers=headers).json()
            nodes = data.get('Nodes', [])
            nodes_count = int(data.get('TotalNodes', len(nodes)))
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
            nodes, node_count = cls.get_cluster_nodes()
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
    def list_directory(cls, root_path: str, rel_path: str) -> List[ydb.SchemeEntry]:
        path = f'{root_path}/{rel_path}' if root_path else rel_path
        LOGGER.info(f'list {path}')
        result = []
        for child in cls.get_ydb_driver().scheme_client.list_directory(path).children:
            if child.name == '.sys':
                continue
            child.name = f'{rel_path}/{child.name}'
            result.append(child)
            if child.is_directory() or child.is_column_store():
                result += cls.list_directory(root_path, child.name)
        return result

    @classmethod
    def _describe_path_impl(cls, path: str) -> ydb.SchemeEntry:
        LOGGER.info(f'describe {path}')
        return cls.get_ydb_driver().scheme_client.describe_path(path)

    @classmethod
    def _get_tables(cls, path):
        full_path = f'/{cls.ydb_database}/{path}'
        LOGGER.info(f'get_tables {full_path}')
        result = []
        self_descr = cls._describe_path_impl(full_path)
        if self_descr is not None:
            if self_descr.is_directory():
                for descr in cls.list_directory('', full_path):
                    if descr.is_any_table():
                        result.append(descr.name)
            elif self_descr.is_any_table():
                result.append(full_path)
        return result

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
    def check_if_ydb_alive(cls, timeout=10, balanced_paths=None):
        def _check_node(n):
            name = 'UnknownNode'
            error = None
            role = 'Unknown'
            try:
                ss = n.get('SystemState', {})
                name = ss.get("Host")
                start_time = int(ss.get('StartTime', int(time()) * 1000)) / 1000
                uptime = int(time()) - start_time
                r = ss.get('Roles', [])
                for role_candidate in ['Storage', 'Tenant']:
                    if role_candidate in r:
                        role = role_candidate
                        break
                if uptime < 15:
                    error = f'Node {name} too yong: {uptime}'
            except BaseException as ex:
                error = f"Error while process node {name}: {ex}"
            if error:
                LOGGER.error(error)
            return error, role

        errors = []
        try:
            nodes, node_count = cls.get_cluster_nodes()
            if node_count == 0:
                errors.append('nodes_count == 0')
            if len(nodes) < node_count:
                errors.append(f"{node_count - len(nodes)} nodes from {node_count} don't live")
            ok_by_role = {'Tenant': 0, 'Storage': 0, 'Unknown': 0}
            nodes_by_role = deepcopy(ok_by_role)
            node_errors = {'Tenant': [], 'Storage': [], 'Unknown': []}
            for n in nodes:
                error, role = _check_node(n)
                if error:
                    node_errors[role].append(error)
                else:
                    ok_by_role[role] += 1
                nodes_by_role[role] += 1
            dynnodes_count = nodes_by_role['Tenant']
            ok_dynnodes_count = ok_by_role['Tenant']
            if ok_dynnodes_count < dynnodes_count:
                dynnodes_errors = ','.join(node_errors['Tenant'])
                errors.append(f'Only {ok_dynnodes_count} from {dynnodes_count} dynnodes are ok: {dynnodes_errors}')
            storage_nodes_count = nodes_by_role['Storage']
            ok_storage_nodes_count = ok_by_role['Storage']
            if ok_storage_nodes_count < storage_nodes_count:
                storage_nodes_errors = ','.join(node_errors['Tenant'])
                errors.append(f'Only {ok_storage_nodes_count} from {storage_nodes_count} storage nodes are ok. {storage_nodes_errors}')
            paths_to_balance = []
            if isinstance(balanced_paths, str):
                paths_to_balance += cls._get_tables(balanced_paths)
            elif isinstance(balanced_paths, list):
                for path in balanced_paths:
                    paths_to_balance += cls._get_tables(path)
            if os.getenv('TEST_CHECK_BALANCING', 'no') == 'yes':
                for p in paths_to_balance:
                    table_nodes, _ = cls.get_cluster_nodes(p)
                    min = None
                    max = None
                    for tn in table_nodes:
                        tablet_count = 0
                        for tablet in tn.get("Tablets", []):
                            if tablet.get("State") != "Green":
                                errors.append(f'Node {tn.get("SystemState", {}).get("Host")}: {tablet.get("Count")} tablets of type {tablet.get("Type")} in {tablet.get("State")} state')
                            if tablet.get("Type") in {"ColumnShard", "DataShard"}:
                                tablet_count += tablet.get("Count")
                        if tablet_count > 0:
                            if min is None or tablet_count < min:
                                min = tablet_count
                            if max is None or tablet_count > max:
                                max = tablet_count
                    if min is None or max is None:
                        errors.append(f'Table {p} has no tablets')
                    elif max - min > 1:
                        errors.append(f'Table {p} is not balanced: {min}-{max} shards.')
                    LOGGER.info(f'Table {p} is balanced: {min}-{max} shards.')

            cls.execute_single_result_query("select 1", timeout)
        except BaseException as ex:
            errors.append(f"Cannot connect to YDB: {ex}")
        if len(errors) == 0:
            return None
        error = ', '.join(errors)
        LOGGER.error(error)
        return error

    @classmethod
    @allure.step('Wait YDB alive')
    def wait_ydb_alive(cls, timeout=10, balanced_paths=None):
        deadline = time() + timeout
        error = None
        while time() < deadline:
            error = cls.check_if_ydb_alive(deadline - time(), balanced_paths=balanced_paths)
            if error is None:
                break
            sleep(1)
        return error
