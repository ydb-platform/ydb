from __future__ import annotations
import allure
import logging
import os
import requests
import yaml
import ydb
import subprocess
import yatest
import socket
import shutil
from ydb.tests.olap.lib.utils import get_external_param
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

        def execute_command(self, cmd: Union[str, list], raise_on_error: bool = True,
                            timeout: Optional[float] = None, raise_on_timeout: bool = True) -> tuple[str, str]:
            """
            Выполняет команду на ноде через SSH или локально

            Args:
                cmd: команда для выполнения (строка или список)
                raise_on_error: вызывать ли исключение при ошибке
                timeout: таймаут выполнения команды в секундах
                raise_on_timeout: вызывать ли исключение при таймауте (по умолчанию True)

            Returns:
                tuple[str, str]: (stdout, stderr) - вывод команды
            """
            
            def _handle_timeout_error(e: yatest.common.ExecutionTimeoutError, 
                                    full_cmd: Union[str, list], 
                                    is_local: bool) -> tuple[str, str]:
                """Обрабатывает ошибки таймаута"""
                cmd_type = "Local" if is_local else "SSH"
                timeout_info = f"{cmd_type} command timed out after {timeout} seconds on {self.host}"
                stdout = ""
                stderr = ""

                # Извлекаем информацию из execution_result
                if hasattr(e, 'execution_result') and e.execution_result:
                    execution_obj = e.execution_result
                    if hasattr(execution_obj, 'std_out') and execution_obj.std_out:
                        stdout = YdbCluster._safe_decode(execution_obj.std_out)
                    if hasattr(execution_obj, 'std_err') and execution_obj.std_err:
                        stderr = YdbCluster._safe_decode(execution_obj.std_err)
                        if not is_local:
                            stderr = YdbCluster._filter_ssh_warnings(stderr)
                    if hasattr(execution_obj, 'command'):
                        timeout_info += f"\nCommand: {execution_obj.command}"

                # Логирование
                if raise_on_timeout:
                    LOGGER.error(f"{cmd_type} command timed out after {timeout} seconds on {self.host}")
                    LOGGER.error(f"Full command: {full_cmd}")
                    LOGGER.error(f"Original command: {cmd}")
                else:
                    LOGGER.warning(f"{cmd_type} command timed out after {timeout} seconds on {self.host}")
                    LOGGER.info(f"Full command: {full_cmd}")
                    LOGGER.info(f"Original command: {cmd}")

                if stdout or stderr:
                    LOGGER.info(f"Partial stdout before timeout:\n{stdout}")
                    LOGGER.info(f"Partial stderr before timeout:\n{stderr}")
                else:
                    LOGGER.debug("No partial output available")

                if raise_on_timeout:
                    raise subprocess.TimeoutExpired(full_cmd, timeout, output=stdout, stderr=stderr)

                return (f"{timeout_info}\n{stdout}" if stdout else timeout_info, stderr)

            def _handle_execution_error(e: yatest.common.ExecutionError, 
                                      full_cmd: Union[str, list], 
                                      is_local: bool) -> tuple[str, str]:
                """Обрабатывает ошибки выполнения"""
                cmd_type = "Local" if is_local else "SSH"
                stdout = ""
                stderr = ""
                exit_code = 1

                # Извлекаем информацию из execution_result
                if hasattr(e, 'execution_result') and e.execution_result:
                    execution_obj = e.execution_result
                    if hasattr(execution_obj, 'std_out') and execution_obj.std_out:
                        stdout = YdbCluster._safe_decode(execution_obj.std_out)
                    if hasattr(execution_obj, 'std_err') and execution_obj.std_err:
                        stderr = YdbCluster._safe_decode(execution_obj.std_err)
                    if hasattr(execution_obj, 'exit_code'):
                        exit_code = execution_obj.exit_code
                    elif hasattr(execution_obj, 'returncode'):
                        exit_code = execution_obj.returncode

                # Фильтруем SSH предупреждения для удаленных команд
                if not is_local:
                    stderr = YdbCluster._filter_ssh_warnings(stderr)

                if raise_on_error:
                    # Логируем детальную информацию об ошибке
                    LOGGER.error(f"{cmd_type} command failed with exit code {exit_code} on {self.host}")
                    LOGGER.error(f"Full command: {full_cmd}")
                    LOGGER.error(f"Original command: {cmd}")
                    if stdout:
                        LOGGER.error(f"Command stdout:\n{stdout}")
                    if stderr:
                        LOGGER.error(f"Command stderr:\n{stderr}")
                    
                    raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)
                
                LOGGER.error(f"Error executing {cmd_type.lower()} command on {self.host}: {e}")
                if stdout:
                    LOGGER.error(f"Command stdout:\n{stdout}")
                if stderr:
                    LOGGER.error(f"Command stderr:\n{stderr}")
                
                return stdout, stderr

            def _execute_local_command(cmd: Union[str, list]) -> tuple[str, str]:
                """Выполняет команду локально"""
                LOGGER.info(f"Detected localhost ({self.host}), executing command locally: {cmd}")
                
                full_cmd = cmd
                try:
                    execution = yatest.common.execute(
                        full_cmd,
                        wait=True,
                        check_exit_code=False,
                        timeout=timeout,
                        shell=isinstance(cmd, str)
                    )

                    stdout = (YdbCluster._safe_decode(execution.std_out)
                              if hasattr(execution, 'std_out') and execution.std_out else "")
                    stderr = (YdbCluster._safe_decode(execution.std_err)
                              if hasattr(execution, 'std_err') and execution.std_err else "")

                    exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
                    if exit_code != 0 and raise_on_error:
                        raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)

                    return stdout, stderr

                except yatest.common.ExecutionTimeoutError as e:
                    return _handle_timeout_error(e, full_cmd, is_local=True)
                except yatest.common.ExecutionError as e:
                    return _handle_execution_error(e, full_cmd, is_local=True)
                except Exception as e:
                    if raise_on_error:
                        raise
                    LOGGER.error(f"Unexpected error executing local command on {self.host}: {e}")
                    return "", ""

            def _execute_ssh_command(cmd: Union[str, list]) -> tuple[str, str]:
                """Выполняет команду через SSH"""
                LOGGER.info(f"Executing SSH command on {self.host}: {cmd}")
                
                ssh_cmd = ['ssh', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

                # Добавляем SSH пользователя и ключ
                ssh_user = os.getenv('SSH_USER')
                if ssh_user is not None:
                    ssh_cmd += ['-l', ssh_user]
                ssh_key_file = os.getenv('SSH_KEY_FILE')
                if ssh_key_file is not None:
                    ssh_cmd += ['-i', ssh_key_file]

                if isinstance(cmd, list):
                    full_cmd = ssh_cmd + [self.host] + cmd
                else:
                    full_cmd = ssh_cmd + [self.host, cmd]

                try:
                    execution = yatest.common.execute(
                        full_cmd,
                        wait=True,
                        check_exit_code=False,
                        timeout=timeout
                    )

                    stdout = (YdbCluster._safe_decode(execution.std_out)
                              if hasattr(execution, 'std_out') and execution.std_out else "")
                    stderr = (YdbCluster._safe_decode(execution.std_err)
                              if hasattr(execution, 'std_err') and execution.std_err else "")

                    stderr = YdbCluster._filter_ssh_warnings(stderr)

                    exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
                    if exit_code != 0 and raise_on_error:
                        raise subprocess.CalledProcessError(exit_code, full_cmd, stdout, stderr)

                    return stdout, stderr

                except yatest.common.ExecutionTimeoutError as e:
                    return _handle_timeout_error(e, full_cmd, is_local=False)
                except yatest.common.ExecutionError as e:
                    return _handle_execution_error(e, full_cmd, is_local=False)
                except Exception as e:
                    if raise_on_error:
                        raise
                    LOGGER.error(f"Unexpected error executing SSH command on {self.host}: {e}")
                    return "", ""

            # Основная логика: выбираем локальное или SSH выполнение
            if YdbCluster._is_localhost(self.host):
                return _execute_local_command(cmd)
            else:
                return _execute_ssh_command(cmd)

        def copy_file(self, local_path: str, remote_path: str, raise_on_error: bool = True) -> str:
            """
            Копирует файл на ноду

            Args:
                local_path: путь к локальному файлу
                remote_path: путь на ноде
                raise_on_error: вызывать ли исключение при ошибке

            Returns:
                str: вывод команды копирования
            """
            return YdbCluster.copy_file_to_remote(local_path, self.host, remote_path, raise_on_error)

        def mkdir(self, path: str, raise_on_error: bool = False) -> str:
            """
            Создает директорию на ноде

            Args:
                path: путь к создаваемой директории
                raise_on_error: вызывать ли исключение при ошибке

            Returns:
                str: вывод команды
            """
            stdout, stderr = self.execute_command(f"mkdir -p {path}", raise_on_error)
            return stdout

        def chmod(self, path: str, mode: str = "+x", raise_on_error: bool = True) -> str:
            """
            Изменяет права доступа к файлу на ноде

            Args:
                path: путь к файлу
                mode: права доступа (по умолчанию +x)
                raise_on_error: вызывать ли исключение при ошибке

            Returns:
                str: вывод команды
            """
            stdout, stderr = self.execute_command(f"chmod {mode} {path}", raise_on_error)
            return stdout

        def deploy_binary(self, local_path: str, target_dir: str, make_executable: bool = True) -> Dict[str, Any]:
            """
            Разворачивает бинарный файл на ноде

            Args:
                local_path: путь к локальному бинарному файлу
                target_dir: директория на ноде
                make_executable: делать ли файл исполняемым

            Returns:
                Dict: результат деплоя
            """
            binary_name = os.path.basename(local_path)
            target_path = os.path.join(target_dir, binary_name)
            result = {
                'name': binary_name,
                'path': target_path,
                'success': False
            }

            try:
                # Создаем директорию
                self.mkdir(target_dir)

                # Копируем файл
                self.copy_file(local_path, target_path)

                # Делаем файл исполняемым, если нужно
                if make_executable:
                    self.chmod(target_path)

                # Проверяем, что файл скопирован успешно
                stdout, stderr = self.execute_command(f"ls -la {target_path}")

                result.update({
                    'success': True,
                    'output': stdout
                })

                return result
            except Exception as e:
                result.update({
                    'error': str(e)
                })
                return result

    _ydb_driver = None
    _results_driver = None
    _cluster_info = None
    ydb_endpoint = get_external_param('ydb-endpoint', 'grpc://ydb-olap-testing-vla-0002.search.yandex.net:2135')
    ydb_database = get_external_param('ydb-db', 'olap-testing/kikimr/testing/acceptance-2').lstrip('/')
    ydb_mon_port = 8765
    tables_path = get_external_param('tables-path', 'olap_yatests')
    _monitoring_urls: list[YdbCluster.MonitoringUrl] = None
    _dyn_nodes_count: Optional[int] = None

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
        try:
            url = f'{cls._get_service_url()}/viewer/json/nodes?'
            if db_only or path is not None:
                url += f'database=/{cls.ydb_database}'
            if path is not None:
                url += f'&path={path}&tablets=true'
            headers = {}
            response = requests.get(url, headers=headers)
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
        except requests.HTTPError as e:
            LOGGER.error(f'{e.strerror}: {e.response.content}')
        except Exception as e:
            LOGGER.error(e)
        return []

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
    def reset(cls, ydb_endpoint, ydb_database, ydb_mon_port, dyn_nodes_count):
        cls.ydb_endpoint = ydb_endpoint
        cls.ydb_database = ydb_database
        cls.ydb_mon_port = ydb_mon_port
        cls._dyn_nodes_count = dyn_nodes_count
        cls._ydb_driver = None

    @classmethod
    def get_ydb_driver(cls):
        if cls._ydb_driver is None:
            cls._ydb_driver = cls._create_ydb_driver(
                cls.ydb_endpoint, cls.ydb_database, oauth=os.getenv('OLAP_YDB_OAUTH', None)
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
    @allure.step('Execute raw upsert query')
    def execute_raw_upsert_query(cls, query, timeout=10):
        """
        Выполняет произвольный upsert запрос в YDB, переданный как строка.

        Args:
            query (str): Полный SQL запрос UPSERT.
            timeout (int): Таймаут выполнения запроса в секундах.

        Returns:
            bool: True если операция выполнена успешно.

        Raises:
            Exception: Если произошла ошибка при выполнении запроса.
        """
        # Прикрепляем запрос к отчету Allure
        allure.attach(query, 'raw upsert query', attachment_type=allure.attachment_type.TEXT)

        try:
            # Создаем сессию
            session = cls.get_ydb_driver().table_client.session().create()

            # Устанавливаем таймаут
            settings = ydb.BaseRequestSettings().with_timeout(timeout)

            # Выполняем запрос
            session.transaction().execute(
                query,
                settings=settings,
                commit_tx=True
            )

            LOGGER.info("Successfully executed upsert query")
            return True
        except Exception as e:
            LOGGER.error(f"Error during raw upsert into YDB: {str(e)}")
            raise

    @classmethod
    @allure.step('Create table in YDB')
    def create_table(cls, ddl_query: str) -> Dict[str, Any]:
        """
        Создает таблицу в YDB используя DDL запрос

        Args:
            ddl_query: DDL запрос для создания таблицы
            session_timeout: Таймаут сессии в секундах

        Returns:
            Dict: Результат операции с информацией об успехе или ошибке
        """
        result = {
            'success': False,
            'query': ddl_query,
            'timestamp': time(),
        }

        try:
            # Получаем драйвер YDB
            driver = cls.get_ydb_driver()

            allure.attach(ddl_query, "DDL Query", attachment_type=allure.attachment_type.TEXT)
            LOGGER.info(f"Executing DDL query:\n{ddl_query}")

            # Создаем сессию
            session = driver.table_client.session().create()

            # Выполняем DDL запрос
            session.execute_scheme(ddl_query)

            # Если запрос выполнился без ошибок, помечаем операцию как успешную
            result['success'] = True
            result['message'] = "Table created successfully"
            LOGGER.info("Table created successfully")

        except Exception as e:
            # Обрабатываем возможные ошибки
            error_message = str(e)
            LOGGER.error(f"Error creating table: {error_message}")
            result['error'] = error_message
            result['exception_type'] = type(e).__name__

            # Прикрепляем информацию об ошибке к отчету
            allure.attach(error_message, "Error creating table", attachment_type=allure.attachment_type.TEXT)

        return result

    @classmethod
    def table_exists(cls, table_path: str) -> bool:
        """
        Проверяет существование таблицы по указанному пути

        Args:
            table_path: Путь к таблице

        Returns:
            bool: True если таблица существует, False в противном случае
        """
        try:
            # Получаем описание объекта по пути
            obj = cls._describe_path_impl(table_path)

            # Проверяем, является ли объект таблицей
            return obj is not None and obj.is_any_table()

        except Exception as e:
            LOGGER.error(f"Error checking if table exists: {e}")
            return False

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

    @staticmethod
    def _safe_decode(data) -> str:
        """
        Безопасно декодирует данные в строку

        Args:
            data: данные для декодирования (str, bytes или None)

        Returns:
            str: декодированная строка
        """
        if data is None:
            return ""
        if isinstance(data, bytes):
            return data.decode('utf-8', errors='replace')
        return str(data)

    @staticmethod
    def _filter_ssh_warnings(stderr: str) -> str:
        """
        Фильтрует SSH предупреждения из stderr

        Args:
            stderr: строка с выводом stderr

        Returns:
            str: отфильтрованная строка stderr без SSH предупреждений
        """
        if not stderr:
            return stderr

        # Фильтруем строки, начинающиеся с "Warning: Permanently added"
        filtered_lines = []
        for line in stderr.splitlines():
            if (not line.startswith('Warning: Permanently added') and not line.startswith('(!) New version of YDB CL')):
                filtered_lines.append(line)

        return '\n'.join(filtered_lines)

    @staticmethod
    def copy_file_to_remote(local_path: str, remote_host: str, remote_path: str,
                            raise_on_error: bool = True) -> str:
        """
        Копирует файл на удаленный хост через SCP или локально

        Args:
            local_path: путь к локальному файлу
            remote_host: имя удаленного хоста
            remote_path: путь на удаленном хосте
            raise_on_error: вызывать ли исключение при ошибке

        Returns:
            str: вывод команды копирования
        """
        # Проверяем существование локального файла
        if not os.path.exists(local_path):
            error_msg = f"Local file does not exist: {local_path}"
            LOGGER.error(error_msg)
            if raise_on_error:
                raise FileNotFoundError(error_msg)
            return None

        # Логируем размер файла для диагностики
        try:
            file_size = os.path.getsize(local_path)
            LOGGER.debug(f"File size: {file_size} bytes")
        except OSError as e:
            LOGGER.warning(f"Could not get file size: {e}")

        # Проверяем, является ли хост localhost
        if YdbCluster._is_localhost(remote_host):
            LOGGER.info(f"Detected localhost ({remote_host}), copying file locally: {local_path} -> {remote_path}")
            
            try:
                # Создаем директорию назначения, если она не существует
                remote_dir = os.path.dirname(remote_path)
                if remote_dir and not os.path.exists(remote_dir):
                    os.makedirs(remote_dir, exist_ok=True)
                    LOGGER.debug(f"Created directory: {remote_dir}")
                
                # Проверяем, не является ли целевой файл занятым
                if os.path.exists(remote_path):
                    try:
                        # Пытаемся открыть файл для записи, чтобы проверить, не занят ли он
                        with open(remote_path, 'r+b'):
                            pass
                    except (OSError, IOError) as e:
                        if "Text file busy" in str(e) or "Resource temporarily unavailable" in str(e):
                            # Генерируем новое имя файла с постфиксом
                            timestamp = int(time())
                            remote_filename = os.path.basename(remote_path)
                            new_remote_filename = f"{remote_filename}.{timestamp}"
                            remote_path = os.path.join(remote_dir, new_remote_filename)
                            LOGGER.warning(f"Target file is busy, using new filename: {remote_path}")
                
                # Копируем файл
                shutil.copy2(local_path, remote_path)
                
                # Проверяем, что файл скопирован успешно
                if os.path.exists(remote_path):
                    copied_size = os.path.getsize(remote_path)
                    original_size = os.path.getsize(local_path)
                    if copied_size == original_size:
                        LOGGER.info(f"Successfully copied file locally: {local_path} -> {remote_path}")
                        return f"Local copy successful: {remote_path}"
                    else:
                        error_msg = f"File size mismatch after copy: original={original_size}, copied={copied_size}"
                        LOGGER.error(error_msg)
                        if raise_on_error:
                            raise IOError(error_msg)
                        return None
                else:
                    error_msg = f"File was not created at destination: {remote_path}"
                    LOGGER.error(error_msg)
                    if raise_on_error:
                        raise IOError(error_msg)
                    return None
                    
            except Exception as e:
                error_msg = f"Error copying file locally: {e}"
                LOGGER.error(error_msg)
                if raise_on_error:
                    raise IOError(error_msg) from e
                return None

        # Для удаленных хостов используем SCP
        LOGGER.info(f"Copying {local_path} to {remote_host} via SCP")
        
        scp_cmd = ['scp', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

        # Добавляем SSH пользователя, если указан
        ssh_user = os.getenv('SSH_USER')
        scp_host = remote_host
        if ssh_user is not None:
            scp_host = f"{ssh_user}@{scp_host}"

        # Добавляем ключ SSH, если указан
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file is not None:
            scp_cmd += ['-i', ssh_key_file]

        def _try_copy(target_path: str) -> tuple[bool, str, str]:
            """
            Попытка копирования файла
            
            Returns:
                tuple[bool, str, str]: (success, stdout, stderr_filtered)
            """
            cmd = scp_cmd + [local_path, f"{scp_host}:{target_path}"]
            LOGGER.info(f"Copying {local_path} to {scp_host}:{target_path}")

            try:
                execution = yatest.common.execute(
                    cmd,
                    wait=True,
                    check_exit_code=False,  # Проверяем exit code сами
                )

                # Декодируем stdout и stderr отдельно
                stdout = (YdbCluster._safe_decode(execution.std_out)
                          if hasattr(execution, 'std_out') and execution.std_out else "")
                stderr = (YdbCluster._safe_decode(execution.std_err)
                          if hasattr(execution, 'std_err') and execution.std_err else "")

                # Фильтруем SSH предупреждения из stderr
                stderr_filtered = YdbCluster._filter_ssh_warnings(stderr)

                # Логируем детальную информацию
                if stdout:
                    LOGGER.debug(f"SCP stdout: {stdout}")
                if stderr_filtered:
                    LOGGER.warning(f"SCP stderr: {stderr_filtered}")

                # Проверяем exit code
                exit_code = getattr(execution, 'exit_code', getattr(execution, 'returncode', 0))
                if exit_code == 0:
                    return True, stdout, stderr_filtered

                return False, stdout, stderr_filtered

            except yatest.common.ExecutionError as e:
                # Возвращаем реальные stdout и stderr из результата выполнения
                stdout = YdbCluster._safe_decode(e.execution_result.std_out)
                stderr = YdbCluster._safe_decode(e.execution_result.std_err)
                stderr_filtered = YdbCluster._filter_ssh_warnings(stderr)

                if stderr_filtered:
                    LOGGER.warning(f"SCP stderr: {stderr_filtered}")

                return False, stdout, stderr_filtered

            except Exception as e:
                LOGGER.error(f"Unexpected error during SCP: {e}")
                return False, "", str(e)

        # Первая попытка копирования с оригинальным именем
        success, stdout, stderr_filtered = _try_copy(remote_path)
        
        if success:
            return stdout

        # Проверяем, является ли ошибка "Text file busy"
        if "Text file busy" in stderr_filtered:
            LOGGER.warning(f"File {remote_path} is busy, trying with postfix")
            
            # Генерируем новое имя файла с постфиксом
            timestamp = int(time())
            
            # Разделяем путь на директорию и имя файла
            remote_dir = os.path.dirname(remote_path)
            remote_filename = os.path.basename(remote_path)
            
            # Добавляем постфикс к имени файла
            new_remote_filename = f"{remote_filename}.{timestamp}"
            new_remote_path = os.path.join(remote_dir, new_remote_filename)
            
            LOGGER.info(f"Retrying copy with new filename: {new_remote_path}")

            # Вторая попытка с новым именем
            success, stdout, stderr_filtered = _try_copy(new_remote_path)

            if success:
                LOGGER.info(f"Successfully copied file with postfix: {new_remote_path}")
                return stdout

        # Если копирование не удалось, обрабатываем ошибку
        error_msg = "SCP command failed with exit code 1"
        if stderr_filtered:
            error_msg += f". Error: {stderr_filtered}"

            # Анализируем частые ошибки для более понятной диагностики
            if "Permission denied" in stderr_filtered:
                error_msg += "\nPossible causes: SSH key authentication failed"
            elif "No such file or directory" in stderr_filtered:
                error_msg += "\nPossible causes: Remote directory doesn't exist or remote host unreachable"
            elif "Connection refused" in stderr_filtered:
                error_msg += "\nPossible causes: SSH service not running on remote host or wrong port"
            elif "Host key verification failed" in stderr_filtered:
                error_msg += "\nPossible causes: SSH host key verification issue"
            elif "Network is unreachable" in stderr_filtered:
                error_msg += "\nPossible causes: Network connectivity issue to remote host"

        LOGGER.error(error_msg)

        if raise_on_error:
            raise subprocess.CalledProcessError(
                1,
                scp_cmd + [local_path, f"{scp_host}:{remote_path}"],
                stdout,
                stderr_filtered
            )
        return None

    @classmethod
    @allure.step('Deploy binaries to cluster nodes')
    def deploy_binaries_to_nodes(
        cls,
        binary_files: list,
        target_dir: str = '/tmp/binaries/'
    ) -> Dict[str, Dict[str, Any]]:
        """
        Разворачивает бинарные файлы на всех нодах кластера

        Args:
            binary_files: список путей к бинарным файлам
            target_dir: директория для размещения файлов на нодах

        Returns:
            Dict: словарь с результатами деплоя по хостам
        """
        results = {}
        node_hosts_set = set()

        for node in cls.get_cluster_nodes(db_only=True):
            if node.host not in node_hosts_set:
                node_hosts_set.add(node.host)
            else:
                continue
            node_results = {}
            allure.attach(f"Node: {node.host}", "Node Info", attachment_type=allure.attachment_type.TEXT)

            # Создаем директорию на ноде
            node.mkdir(target_dir)

            # Копируем каждый бинарный файл
            for binary_file in binary_files:
                try:
                    result = node.deploy_binary(binary_file, target_dir)
                    node_results[os.path.basename(binary_file)] = result

                    if result['success']:
                        allure.attach(
                            f"Successfully deployed {result['name']} to {node.host}:"
                            f"{result['path']}\n{result.get('output', '')}",
                            f"Deploy {result['name']} to {node.host}",
                            attachment_type=allure.attachment_type.TEXT
                        )
                    else:
                        allure.attach(
                            f"Failed to deploy {result['name']} to {node.host}: "
                            f"{result.get('error', 'Unknown error')}",
                            f"Deploy {result['name']} to {node.host} failed",
                            attachment_type=allure.attachment_type.TEXT
                        )
                except Exception as e:
                    error_msg = str(e)
                    node_results[os.path.basename(binary_file)] = {
                        'success': False,
                        'error': error_msg
                    }

                    allure.attach(
                        f"Exception when deploying {os.path.basename(binary_file)} "
                        f"to {node.host}: {error_msg}",
                        f"Deploy {os.path.basename(binary_file)} to {node.host} failed",
                        attachment_type=allure.attachment_type.TEXT
                    )

            results[node.host] = node_results

        return results

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
            nodes = cls.get_cluster_nodes(db_only=True)
            expected_nodes_count = cls.get_dyn_nodes_count()
            nodes_count = len(nodes)
            if expected_nodes_count:
                LOGGER.debug(f'Expected nodes count: {expected_nodes_count}')
                if nodes_count < expected_nodes_count:
                    errors.append(f"{expected_nodes_count - nodes_count} nodes from "
                                  f"{expected_nodes_count} don't alive")
            ok_node_count = 0
            node_errors = []
            for n in nodes:
                error = _check_node(n)
                if error:
                    node_errors.append(error)
                else:
                    ok_node_count += 1
            if ok_node_count < nodes_count:
                errors.append(f'Only {ok_node_count} from {nodes_count} dynnodes are ok: '
                              f'{",".join(node_errors)}')
            paths_to_balance = []
            if isinstance(balanced_paths, str):
                paths_to_balance += cls._get_tables(balanced_paths)
            elif isinstance(balanced_paths, list):
                for path in balanced_paths:
                    paths_to_balance += cls._get_tables(path)
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
                            warnings.append(f'Node {tn.host}: {tablet.count} tablets of type '
                                            f'{tablet.type} in {tablet.state} state')
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

    @staticmethod
    def _is_localhost(hostname: str) -> bool:
        """
        Проверяет, является ли хост localhost

        Args:
            hostname: имя хоста для проверки

        Returns:
            bool: True если хост является localhost
        """
        if not hostname:
            return False

        # Очевидные случаи localhost
        localhost_names = {
            'localhost',
            '127.0.0.1',
            '::1',
            socket.gethostname(),
            socket.getfqdn(),
        }

        if hostname in localhost_names:
            return True

        try:
            # Получаем IP адрес хоста
            host_ip = socket.gethostbyname(hostname)

            # Получаем локальные IP адреса
            local_ips = set()

            # Добавляем localhost адреса
            local_ips.update(['127.0.0.1', '::1'])

            # Получаем IP адреса всех сетевых интерфейсов
            hostname_local = socket.gethostname()
            try:
                local_ips.add(socket.gethostbyname(hostname_local))
            except socket.gaierror:
                pass

            # Проверяем, совпадает ли IP хоста с локальными IP
            return host_ip in local_ips

        except (socket.gaierror, socket.herror):
            # Если не можем разрешить имя хоста, считаем что это не localhost
            return False

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
                    stdout, stderr = node.execute_command(ps_cmd, raise_on_error=False)
                    
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
                        stdout, stderr = node.execute_command(kill_cmd, raise_on_error=False)
                        
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
                            stdout_check, stderr_check = node.execute_command(check_cmd, raise_on_error=False)
                            # kill -0 возвращает 0 если процесс существует
                            if "No such process" not in stderr_check:
                                still_alive.append(pid)
                        
                        # Подсчитываем убитые процессы
                        newly_killed = len(pids_list) - len(still_alive)
                        killed_count += newly_killed
                        pids_list = still_alive
                        
                        LOGGER.info(f"After {signal_name}: {newly_killed} processes killed, {len(still_alive)} still alive on {node.host}")
                        
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
