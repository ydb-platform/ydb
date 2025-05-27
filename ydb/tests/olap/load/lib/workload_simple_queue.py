import allure
import pytest
import os
import stat
import tempfile
import yatest
import logging
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import Enum, auto

from library.python import resource

LOGGER = logging.getLogger(__name__)

STRESS_BINARIES_DEPLOY_PATH = '/tmp/stress_binaries/'
WORKLOAD_BINARY_NAME = 'simple_queue'  # Имя бинарного файла
YDB_CLI_BINARY_NAME = 'ydb_cli'

class TableType(str, Enum):
    """Тип таблицы"""
    ROW = 'row'
    COLUMN = 'column'

class SimpleQueueBase(LoadSuiteBase):
    working_dir = os.path.join(tempfile.gettempdir(), "ydb_stability")
    os.makedirs(working_dir, exist_ok=True)
    

    def _unpack_resource(self, name):
        """Распаковывает ресурс из пакета"""
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        return path_to_unpack
    
    def _unpack_workload_binary(self, workload_binary_name: str):
        """Распаковывает бинарный файл из ресурсов и возвращает путь к нему"""
        return self._unpack_resource(workload_binary_name)
    
    @pytest.mark.parametrize('table_type', [t.value for t in TableType])
    def test_workload_simple_queue(self, table_type: str):
        self.save_nodes_state()
        # Распаковываем бинарный файл из ресурсов
        binary_path = [self._unpack_workload_binary(WORKLOAD_BINARY_NAME), self._unpack_workload_binary(YDB_CLI_BINARY_NAME)]
        
        # Разворачиваем бинарный файл на всех нодах кластера
        deploy_results = YdbCluster.deploy_binaries_to_nodes(binary_path, STRESS_BINARIES_DEPLOY_PATH)
       
        # Для каждой ноды в кластере
        nodes = YdbCluster.get_cluster_nodes() #role=YdbCluster.Node.Role.STORAGE надо получить ROle storage = stat nodes
        node = nodes[0]
        
        with allure.step(f'Running workload on node {node.host} with table type {table_type}'):
            node_host = node.host
            # Проверяем, успешно ли был развернут бинарный файл
            binary_result = deploy_results.get(node_host, {}).get(WORKLOAD_BINARY_NAME, {})
            success = binary_result.get('success', False)
            
            # Инициализируем переменные для результата
            command_result = None
            command_error = None

            # Запускаем бинарный файл на ноде, если он был успешно развернут
            if success:
                target_path = binary_result['path']
                cmd = f"{target_path} --endpoint {YdbCluster.ydb_endpoint} --database {YdbCluster.ydb_database} --duration {self.timeout} --mode {table_type}"
                allure.attach(cmd, 'Command to execute', allure.attachment_type.TEXT)
                LOGGER.info(f"Executing command on node {node.host} (is_local: {node.is_local})")
                
                try:
                    result = node.execute_command(cmd, raise_on_error=True, timeout=int(self.timeout * 1.5), raise_on_timeout=False)
                    LOGGER.info(f"Command executed successfully: {result}")
                    allure.attach(str(result), 'Command execution result', allure.attachment_type.TEXT)
                    command_result = result
                except Exception as e:
                    error_msg = f"Command execution failed: {str(e)}"
                    LOGGER.error(error_msg)
                    allure.attach(error_msg, 'Command execution error', allure.attachment_type.TEXT)
                    allure.attach(str(e), 'Exception details', allure.attachment_type.TEXT)
                    command_error = str(e)
                    raise  # Перебрасываем исключение, чтобы тест упал
            else:
                error_msg = f"Binary deployment failed on node {node.host}. Binary result: {binary_result}"
                LOGGER.error(f"Error: {error_msg}")
                allure.attach(error_msg, 'Binary deployment error', allure.attachment_type.TEXT)
                command_error = error_msg
        with allure.step('Checking scheme state'):
            cli_path = deploy_results.get(node_host, {}).get(YDB_CLI_BINARY_NAME, {})['path']
            result = node.execute_command([cli_path,'--endpoint', f'{YdbCluster.ydb_endpoint}','--database', f'/{YdbCluster.ydb_database}', "scheme", "ls", "-lR"], raise_on_error=False)
            allure.attach(str(result), 'Scheme state', allure.attachment_type.TEXT)
            LOGGER.info(f'res:{result}')
            LOGGER.info(f'path to check:{node.host.split('.')[0]}_0')
                       

        result = YdbCliHelper.WorkloadRunResult()
        
        # Добавляем результаты выполнения команды
        if command_result is not None:
            result.stdout = str(command_result)
            # Проверяем на наличие ошибок в выводе
            if "error" in str(command_result).lower():
                result.add_error(str(command_result))
            # Проверяем на наличие предупреждений
            if "warning" in str(command_result).lower():
                result.add_warning(str(command_result))
        elif command_error is not None:
            # Добавляем ошибку выполнения команды
            result.add_error(command_error)
        
        # Добавляем статистику выполнения
        result.add_stat(f"SimpleQueue_{table_type}", "execution_time", self.timeout)
        result.add_stat(f"SimpleQueue_{table_type}", "table_type", table_type)
        result.add_stat(f"SimpleQueue_{table_type}", "node", node.host)
        
        # Добавляем информацию о выполнении в iterations
        iteration = YdbCliHelper.Iteration()
        iteration.time = self.timeout
        if command_error is not None:
            iteration.error_message = command_error
        elif command_result is not None and "error" in str(command_result).lower():
            iteration.error_message = str(command_result)
        result.iterations[0] = iteration
        
        self.process_query_result(result, f"SimpleQueue_{table_type}", False)

class TestSimpleQueue(SimpleQueueBase):
     """Тест с таймаутом из get_external_param """
     timeout = get_external_param('workload_duration', 100)
