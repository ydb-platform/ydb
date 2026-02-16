from __future__ import annotations
import allure
import logging
import os
import requests
import yaml
import ydb
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.remote_execution import execute_command, deploy_binaries_to_hosts
from copy import deepcopy
from time import sleep, time
from typing import List, Optional, Callable, Dict, Any, Union
from enum import Enum

LOGGER = logging.getLogger()


class YdbCluster:
    class MonitoringUrl:
        def __init__(self, url: str, caption: str = 'link') -> None:
            self.url = url
            if self.url.find('://') < 0:
                self.url = f'https://{self.url}'
            self.caption = caption

    class Node:
        class Role(Enum):
            UNKNOWN = 0
            STORAGE = 1
            COMPUTE = 2

        class Tablet:
            def __init__(self, desc: dict):
                self.state: str = desc.get('State', 'Red')
                self.type: str = desc.get('Type', 'Unknown')
                self.count: int = desc.get('Count', 0)

        def __init__(self, desc: dict):
            ss = desc.get('SystemState', {})
            self.host: str = ss.get('Host', '')
            ports = {
                e.get('Name', ''): int(e.get('Address', '0').split(':')[-1])
                for e in ss.get('Endpoints', [])
            }
            self.ic_port: int = ports.get('ic', 0)
            self.mon_port: int = ports.get('http-mon', 0)
            self.grpc_port: int = ports.get('grpc', 0)
            self.disconnected: bool = desc.get('Disconnected', False)
            self.version: str = ss.get('Version', '')
            self.start_time: float = 0.001 * int(ss.get('StartTime', time() * 1000))
            if 'Storage' in ss.get('Roles', []):
                self.role = YdbCluster.Node.Role.STORAGE
            elif 'Tenants' in ss.get('Roles', []):
                self.role = YdbCluster.Node.Role.COMPUTE
            else:
                self.role = YdbCluster.Node.Role.UNKNOWN
            self.tablets = [YdbCluster.Node.Tablet(t) for t in desc.get('Tablets', [])]

        @property
        def slot(self) -> str:
            role_prefix = "static" if self.role == YdbCluster.Node.Role.STORAGE else self.ic_port
            return f'{role_prefix}@{self.host}'

    _ydb_driver = None
    _results_driver = None
    _cluster_info = None
    ydb_endpoint = get_external_param('ydb-endpoint', 'grpc://ydb-olap-testing-vla-0002.search.yandex.net:2135')
    ydb_database = get_external_param('ydb-db', 'olap-testing/kikimr/testing/acceptance-2').lstrip('/')
    ydb_mon_port = int(get_external_param('ydb-mon-port', 8765))
    ydb_iam_file = get_external_param('ydb-iam-file', os.getenv('YDB_IAM_FILE'))
    _tables_path = get_external_param('tables-path', 'olap_yatests').rstrip('/')
    _monitoring_urls: list[YdbCluster.MonitoringUrl] = None
    _dyn_nodes_count: Optional[int] = None
    _client_host: Optional[str] = None

    @classmethod
    def get_client_host(cls) -> str:
        if not cls._client_host:
            cls._client_host = get_external_param('client-host', 'localhost')
            if cls._client_host == 'static':
                nodes = cls.get_cluster_nodes(role=YdbCluster.Node.Role.STORAGE, db_only=False)
                if not nodes:
                    raise RuntimeError(
                        "client-host is set to 'static', but no STORAGE nodes are available from get_cluster_nodes()"
                    )
                cls._client_host = nodes[0].host
        return cls._client_host

    @classmethod
    def get_tables_path(cls, subpath: str = '') -> str:
        if cls._tables_path and subpath:
            return f'{cls._tables_path}/{subpath}'
        return subpath if subpath else cls._tables_path

    @classmethod
    def get_full_tables_path(cls, subpath: str = '') -> str:
        return f'/{cls.ydb_database}/{cls.get_tables_path(subpath)}'

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
        return f'http://{host}:{cls.ydb_mon_port}'

    @classmethod
    def get_cluster_nodes(cls, path: Optional[str] = None, db_only: bool = False,
                          role: Optional[YdbCluster.Node.Role] = None
                          ) -> list[YdbCluster.Node]:
        if cls.ydb_mon_port == 0:
            return []

        # Получаем таймаут из переменной окружения, как в wait_ydb_alive
        timeout = int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 2 * 60))  # По умолчанию 20 минут
        retry_interval = 2  # Интервал между попытками в секундах

        deadline = time() + timeout
        last_error = None

        while time() < deadline:
            try:
                url = f'{cls._get_service_url()}/viewer/json/nodes?'
                if db_only or path is not None:
                    url += f'database=/{cls.ydb_database}'
                if path is not None:
                    url += f'&path={path}&tablets=true'

                headers = {}
                # Добавляем таймаут для каждого запроса
                request_timeout = min(30, deadline - time())
                if request_timeout <= 0:
                    break

                response = requests.get(url, headers=headers, timeout=request_timeout)
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, dict):
                    raise Exception(f'Incorrect response type: {data}')

                # Create nodes from the response
                nodes = [YdbCluster.Node(n) for n in data.get('Nodes', [])]

                # Filter nodes by role if specified
                if role is not None:
                    nodes = [node for node in nodes if node.role == role]

                return nodes

            except Exception as e:
                last_error = e
                LOGGER.debug(f'Failed to get cluster nodes: {e}. Retrying in {retry_interval}s...')

                # Проверяем, есть ли время для следующей попытки
                if time() + retry_interval >= deadline:
                    break

                sleep(retry_interval)

        # Если все попытки неудачны, логируем ошибку и возвращаем пустой список
        LOGGER.error(f'Failed to get cluster nodes after {timeout}s timeout. Last error: {last_error}')
        return []

    @classmethod
    def get_metrics(cls, metrics: dict[str, dict[str, str]], db_only: bool = False, role: Optional[YdbCluster.Node.Role] = None, counters: str = 'tablets') -> dict[str, dict[str, float]]:
        def sensor_has_labels(sensor, labels: dict[str, str]) -> bool:
            for k, v in labels.items():
                if sensor.get('labels', {}).get(k, '') != v:
                    return False
            return True
        nodes = cls.get_cluster_nodes(db_only=db_only, role=role)
        result = {}
        for node in nodes:
            url = f'http://{node.host}:{node.mon_port}/counters/'
            if counters:
                url += f'counters={counters}/'
            url += 'json'
            response = requests.get(url)
            response.raise_for_status()
            sensor_values = {}
            for name, labels in metrics.items():
                for sensor in response.json()['sensors']:
                    if sensor_has_labels(sensor, labels):
                        sensor_values.setdefault(name, 0.)
                        sensor_values[name] += sensor['value']
            result[node.slot] = sensor_values
        return result

    @classmethod
    def get_cluster_info(cls):
        if cls._cluster_info is None:
            version = ''
            nodes = cls.get_cluster_nodes(db_only=True)
            for node in nodes:
                if not version:
                    version = node.version
            cls._cluster_info = {
                'database': cls.ydb_database,
                'version': version,
                'name': cls.ydb_database.strip('/').split('/')[0],
            }
        return deepcopy(cls._cluster_info)

    @staticmethod
    def _create_ydb_driver(endpoint, database, oauth=None, iam_file=None):
        credentials = None
        LOGGER.info(f"Connecting to {endpoint} to {database} ydb_access_token is set {oauth is not None}, iam file path is {iam_file}")

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
    def reset(cls, ydb_endpoint, ydb_database, ydb_mon_port, dyn_nodes_count):
        cls.ydb_endpoint = ydb_endpoint
        cls.ydb_database = ydb_database
        cls.ydb_mon_port = ydb_mon_port
        cls._dyn_nodes_count = dyn_nodes_count
        cls._ydb_driver = None
        cls._client_host = None

    @classmethod
    def get_ydb_driver(cls):
        if cls._ydb_driver is None:
            cls._ydb_driver = cls._create_ydb_driver(
                cls.ydb_endpoint, cls.ydb_database, oauth=os.getenv('OLAP_YDB_OAUTH', None), iam_file=cls.ydb_iam_file
            )
        return cls._ydb_driver

    @classmethod
    def list_directory(cls, root_path: str, rel_path: str,
                       kind_order_key: Optional[Callable[[ydb.SchemeEntryType], int]] = None
                       ) -> List[ydb.SchemeEntry]:
        path = f'{root_path}/{rel_path}' if root_path else rel_path
        LOGGER.info(f'list {path}')
        result = []
        entries = cls.get_ydb_driver().scheme_client.list_directory(path).children
        if kind_order_key is not None:
            entries = sorted(entries, key=lambda x: kind_order_key(x.type))
        for child in entries:
            if child.name == '.sys':
                continue
            child.name = f'{rel_path}/{child.name}'
            result.append(child)
            if child.is_directory() or child.is_column_store():
                result += cls.list_directory(root_path, child.name, kind_order_key)
        return result

    @classmethod
    def _describe_path_impl(cls, path: str) -> ydb.SchemeEntry:
        LOGGER.info(f'describe {path}')
        return cls.get_ydb_driver().scheme_client.describe_path(path)

    @classmethod
    def get_tables(cls, path):
        full_path = cls.get_full_tables_path(path)
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

    @staticmethod
    def _join_errors(log_level: int, errors: list[str]):
        if len(errors) > 0:
            error = ', '.join(errors)
            LOGGER.log(log_level, error)
            return error
        return None

    @classmethod
    @allure.step('Execute scan query')
    def execute_single_result_query(cls, query, timeout=10):
        allure.attach(query, 'query', attachment_type=allure.attachment_type.TEXT)
        settings = ydb.BaseRequestSettings()
        settings = settings.with_timeout(timeout)
        try:
            session = ydb.query.QueryClientSync(cls.get_ydb_driver()).session().create()
            it = session.execute(query, settings=settings)
            result = next(it)
            return result.rows[0][0]
        except BaseException:
            LOGGER.error("Cannot connect to YDB")
            raise

    @classmethod
    def get_dyn_nodes_count(cls) -> int:
        if cls._dyn_nodes_count is None:
            cls._dyn_nodes_count = 0
            if os.getenv('EXPECTED_DYN_NODES_COUNT'):
                cls._dyn_nodes_count = int(os.getenv('EXPECTED_DYN_NODES_COUNT'))
            elif os.getenv('CLUSTER_CONFIG'):
                with open(os.getenv('CLUSTER_CONFIG'), 'r') as r:
                    yaml_config = yaml.safe_load(r.read())
                    for domain in yaml_config['domains']:
                        if domain["domain_name"] == cls.ydb_database:
                            cls._dyn_nodes_count = domain["domain_name"]["dynamic_slots"]
                        for db in domain['databases']:
                            if f'{domain["domain_name"]}/{db["name"]}' == cls.ydb_database:
                                for cu in db['compute_units']:
                                    cls._dyn_nodes_count += cu['count']

        return cls._dyn_nodes_count

    @classmethod
    @allure.step('Deploy binaries to cluster nodes')
    def deploy_binaries_to_nodes(
        cls,
        binary_files: list,
        target_dir: str = '/tmp/stress_binaries/'
    ) -> Dict[str, Dict[str, Any]]:
        """
        Разворачивает бинарные файлы на всех нодах кластера

        Args:
            binary_files: список путей к бинарным файлам
            target_dir: директория для размещения файлов на нодах

        Returns:
            Dict: словарь с результатами деплоя по хостам
        """
        # Получаем уникальные хосты из нод кластера
        nodes = cls.get_cluster_nodes(db_only=True)
        hosts = list(set(node.host for node in nodes))

        # Используем утилитную функцию для развертывания
        return deploy_binaries_to_hosts(binary_files, hosts, target_dir)

    @classmethod
    @allure.step('Check if YDB alive')
    def check_if_ydb_alive(cls, timeout=10, balanced_paths=None) -> tuple[str, str]:
        def _check_node(n: YdbCluster.Node):
            errors = []
            if n.disconnected:
                errors.append(f'Node {n.host} disconnected')
            uptime = time() - n.start_time
            if uptime < 15:
                errors.append(f'Node {n.host} too yong: {uptime}')
            return cls._join_errors(logging.ERROR, errors)

        errors = []
        warnings = []
        try:
            if cls.ydb_mon_port != 0:
                nodes = cls.get_cluster_nodes(db_only=True)
                expected_nodes_count = cls.get_dyn_nodes_count()
                nodes_count = len(nodes)
                if expected_nodes_count:
                    LOGGER.debug(f'Expected nodes count: {expected_nodes_count}')
                    if nodes_count < expected_nodes_count:
                        errors.append(f"{expected_nodes_count - nodes_count} nodes from {expected_nodes_count} don't alive")
                ok_node_count = 0
                node_errors = []
                for n in nodes:
                    error = _check_node(n)
                    if error:
                        node_errors.append(error)
                    else:
                        ok_node_count += 1
                if ok_node_count < nodes_count:
                    errors.append(f'Only {ok_node_count} from {nodes_count} dynnodes are ok: {",".join(node_errors)}')
                paths_to_balance = []
                if isinstance(balanced_paths, str):
                    paths_to_balance += cls.get_tables(balanced_paths)
                elif isinstance(balanced_paths, list):
                    for path in balanced_paths:
                        paths_to_balance += cls.get_tables(path)
                for p in paths_to_balance:
                    table_nodes = cls.get_cluster_nodes(p)
                    min = None
                    max = None
                    if expected_nodes_count:
                        if len(table_nodes) < expected_nodes_count:
                            min = 0
                    for tn in table_nodes:
                        tablet_count = 0
                        for tablet in tn.tablets:
                            if tablet.count > 0 and tablet.state != "Green":
                                warnings.append(f'Node {tn.host}: {tablet.count} tablets of type {tablet.type} in {tablet.state} state')
                            if tablet.type in {"ColumnShard", "DataShard"}:
                                tablet_count += tablet.count
                        if tablet_count > 0:
                            if min is None or tablet_count < min:
                                min = tablet_count
                            if max is None or tablet_count > max:
                                max = tablet_count
                    if min is None or max is None:
                        warnings.append(f'Table {p} has no tablets')
                    elif max - min > 1:
                        warnings.append(f'Table {p} is not balanced: {min}-{max} shards.')
                    LOGGER.info(f'Table {p} balance: {min}-{max} shards.')

            cls.execute_single_result_query("select 1", timeout)
        except BaseException as ex:
            errors.append(f"Cannot connect to YDB: {ex}")

        return cls._join_errors(logging.ERROR, errors), cls._join_errors(logging.WARNING, warnings)

    @classmethod
    @allure.step('Wait YDB alive')
    def wait_ydb_alive(cls, timeout=10, balanced_paths=None):
        deadline = time() + timeout
        error = None
        while time() < deadline:
            error, warning = cls.check_if_ydb_alive(deadline - time(), balanced_paths=balanced_paths)
            if error is None and warning is None:
                break
            sleep(1)
        return error

    @classmethod
    @allure.step('Kill processes on cluster nodes')
    def kill_processes_on_nodes(
        cls,
        process_names: Union[str, list[str]],
        target_dir: Optional[str] = None,
        nodes: Optional[list[YdbCluster.Node]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Останавливает процессы с указанными именами на нодах кластера

        Args:
            process_names: имя процесса или список имен процессов для остановки
            target_dir: директория, в которой искать процессы (опционально)
            nodes: список нод для обработки (если None, используются все ноды кластера)

        Returns:
            Dict: словарь с результатами остановки процессов по хостам
        """
        if isinstance(process_names, str):
            process_names = [process_names]

        if nodes is None:
            nodes = cls.get_cluster_nodes()

        results = {}
        processed_hosts = set()

        for node in nodes:
            # Избегаем дублирования обработки одного хоста
            if node.host in processed_hosts:
                continue
            processed_hosts.add(node.host)

            node_results = {}
            allure.attach(f"Node: {node.host}", "Processing Node", attachment_type=allure.attachment_type.TEXT)

            for process_name in process_names:
                try:
                    result = cls._kill_process_on_node(node, process_name, target_dir)
                    node_results[process_name] = result

                    if result['killed_count'] > 0:
                        allure.attach(
                            f"Successfully killed {result['killed_count']} processes "
                            f"'{process_name}' on {node.host}",
                            f"Kill {process_name} on {node.host}",
                            attachment_type=allure.attachment_type.TEXT
                        )
                    else:
                        allure.attach(
                            f"No processes '{process_name}' found on {node.host}",
                            f"Kill {process_name} on {node.host}",
                            attachment_type=allure.attachment_type.TEXT
                        )

                except Exception as e:
                    error_msg = str(e)
                    node_results[process_name] = {
                        'success': False,
                        'error': error_msg,
                        'killed_count': 0
                    }

                    allure.attach(
                        f"Exception when killing '{process_name}' on {node.host}: {error_msg}",
                        f"Kill {process_name} on {node.host} failed",
                        attachment_type=allure.attachment_type.TEXT
                    )

            results[node.host] = node_results

        return results

    @classmethod
    def _kill_process_on_node(cls, node: YdbCluster.Node, process_name: str,
                              target_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Останавливает процессы с указанным именем на конкретной ноде используя ps и kill

        Args:
            node: нода для обработки
            process_name: имя процесса для остановки
            target_dir: директория, в которой искать процессы (опционально)

        Returns:
            Dict: результат остановки процессов
        """
        result = {
            'success': False,
            'killed_count': 0,
            'found_processes': [],
            'commands_executed': []
        }

        try:
            # Создаем список паттернов для поиска
            search_patterns = [process_name]

            # Если указана директория, добавляем поиск по полному пути
            if target_dir:
                full_path = os.path.join(target_dir, process_name)
                search_patterns.append(full_path)
                # Также ищем любые процессы из указанной директории
                search_patterns.append(target_dir)

            all_found_pids = set()

            for pattern in search_patterns:
                try:
                    # Ищем процессы с помощью ps -aux | grep
                    # Используем [p]attern чтобы исключить сам grep из результатов
                    escaped_pattern = pattern.replace('[', r'\[').replace(']', r'\]')
                    if len(escaped_pattern) > 0:
                        first_char = escaped_pattern[0]
                        rest_pattern = escaped_pattern[1:]
                        grep_pattern = f"[{first_char}]{rest_pattern}"
                    else:
                        grep_pattern = escaped_pattern

                    ps_cmd = f"ps -aux | grep '{grep_pattern}'"
                    result_exec = execute_command(node.host, ps_cmd, raise_on_error=False)
                    stdout = result_exec.stdout
                    stderr = result_exec.stderr

                    result['commands_executed'].append({
                        'command': ps_cmd,
                        'stdout': stdout,
                        'stderr': stderr,
                        'pattern': pattern
                    })

                    if stdout.strip():
                        # Парсим вывод ps для извлечения PID
                        lines = stdout.strip().split('\n')
                        found_pids = []

                        for line in lines:
                            if line.strip():
                                # Формат ps -aux: USER PID %CPU %MEM VSZ RSS TTY STAT START TIME COMMAND
                                parts = line.split(None, 10)  # Разделяем на максимум 11 частей
                                if len(parts) >= 2:
                                    try:
                                        pid = int(parts[1])
                                        command = parts[10] if len(parts) > 10 else ""

                                        # Дополнительная проверка, что это действительно наш процесс
                                        if pattern in command:
                                            found_pids.append(pid)
                                            all_found_pids.add(pid)
                                            result['found_processes'].append({
                                                'pid': pid,
                                                'command': command,
                                                'pattern': pattern,
                                                'user': parts[0] if len(parts) > 0 else "unknown"
                                            })
                                    except (ValueError, IndexError):
                                        # Пропускаем строки, которые не удается распарсить
                                        continue

                        LOGGER.info(f"Found {len(found_pids)} processes matching pattern '{pattern}' on {node.host}")

                except Exception as e:
                    LOGGER.warning(f"Error searching for pattern '{pattern}' on {node.host}: {e}")
                    result['commands_executed'].append({
                        'command': ps_cmd,
                        'error': str(e),
                        'pattern': pattern
                    })

            # Теперь убиваем все найденные процессы
            killed_count = 0
            if all_found_pids:
                pids_list = list(all_found_pids)

                # Сначала пробуем мягкое завершение (SIGTERM)
                for signal_type, signal_name in [('TERM', 'SIGTERM'), ('KILL', 'SIGKILL')]:
                    if not pids_list:  # Если все процессы уже завершены
                        break

                    pids_str = ' '.join(map(str, pids_list))
                    kill_cmd = f"kill -{signal_type} {pids_str}"

                    try:
                        kill_result = execute_command(node.host, kill_cmd, raise_on_error=False)
                        stdout = kill_result.stdout
                        stderr = kill_result.stderr

                        result['commands_executed'].append({
                            'command': kill_cmd,
                            'stdout': stdout,
                            'stderr': stderr,
                            'signal': signal_name
                        })

                        LOGGER.info(f"Sent {signal_name} to {len(pids_list)} processes on {node.host}")

                        # Ждем немного и проверяем, какие процессы еще живы
                        sleep(1)

                        # Проверяем, какие процессы еще существуют
                        still_alive = []
                        for pid in pids_list:
                            check_cmd = f"kill -0 {pid}"
                            stderr_check = execute_command(node.host, check_cmd, raise_on_error=False).stderr
                            # kill -0 возвращает 0 если процесс существует
                            if "No such process" not in stderr_check:
                                still_alive.append(pid)

                        # Подсчитываем убитые процессы
                        newly_killed = len(pids_list) - len(still_alive)
                        killed_count += newly_killed
                        pids_list = still_alive

                        LOGGER.info(
                            f"After {signal_name}: {newly_killed} processes killed,"
                            " {len(still_alive)} still alive on {node.host}"
                        )

                        # Если это был SIGTERM и остались живые процессы, переходим к SIGKILL
                        if signal_type == 'TERM' and still_alive:
                            LOGGER.warning(f"Some processes didn't respond to SIGTERM, will try SIGKILL on {node.host}")
                            sleep(2)  # Даем больше времени перед SIGKILL
                            continue
                        else:
                            break

                    except Exception as e:
                        LOGGER.error(f"Error executing kill command '{kill_cmd}' on {node.host}: {e}")
                        result['commands_executed'].append({
                            'command': kill_cmd,
                            'error': str(e),
                            'signal': signal_name
                        })
                        break

            result['killed_count'] = killed_count
            result['success'] = True

            if killed_count > 0:
                LOGGER.info(f"Successfully killed {killed_count} processes matching '{process_name}' on {node.host}")
            else:
                LOGGER.info(f"No processes matching '{process_name}' found on {node.host}")

        except Exception as e:
            result['error'] = str(e)
            LOGGER.error(f"Error killing processes '{process_name}' on {node.host}: {e}")

        return result
