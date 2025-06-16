import allure
import pytest
import os
import yatest
import logging
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import execute_command, deploy_binaries_to_hosts
from ydb.tests.olap.lib.utils import get_external_param
from enum import Enum

LOGGER = logging.getLogger(__name__)

BINARIES_DEPLOY_PATH = '/tmp/stress_binaries/'
WORKLOAD_BINARY_NAME = 'simple_queue'
YDB_CLI_BINARY_NAME = 'ydb'
YDB_CLI_PATH = yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))


class TableType(str, Enum):
    """Тип таблицы"""
    ROW = 'row'
    COLUMN = 'column'


class SimpleQueueBase(LoadSuiteBase):

    @classmethod
    def do_teardown_class(cls):
        """
        Специфичная очистка для SimpleQueue тестов.
        Останавливает все запущенные workload процессы.
        """
        LOGGER.info("Starting SimpleQueue teardown: stopping workload processes")

        # Останавливаем процессы simple_queue на всех нодах
        cls.kill_workload_processes(
            process_names=[BINARIES_DEPLOY_PATH + WORKLOAD_BINARY_NAME],
            target_dir=BINARIES_DEPLOY_PATH
        )

    @pytest.mark.parametrize('table_type', [t.value for t in TableType])
    def test_workload_simple_queue(self, table_type: str):
        # self.save_nodes_state()
        # Распаковываем бинарные файлы из ресурсов
        binary_files = [
            yatest.common.binary_path(os.getenv("SIMPLE_QUEUE_BINARY"))
        ]

        # Получаем хосты нод кластера
        nodes = YdbCluster.get_cluster_nodes()

        # Выбираем первую ноду для выполнения workload
        node = nodes[0]
        node_host = node.host
        # Деблоим бинарнь
        deploy_results = deploy_binaries_to_hosts(
            binary_files, [node_host], BINARIES_DEPLOY_PATH)

        # Проверяем, успешно ли был развернут бинарный файл
        binary_result = deploy_results.get(node_host, {}).get(WORKLOAD_BINARY_NAME, {})
        success = binary_result.get('success', False)

        # Инициализируем переменные для результата
        command_result = None
        command_error = None

        # Запускаем бинарный файл на ноде, если он был успешно развернут
        if not success:
            error_msg = (
                f"Binary deployment failed on node {node.host}. "
                f"Binary result: {binary_result}"
            )
            LOGGER.error(f"Error: {error_msg}")
            allure.attach(error_msg, 'Binary deployment error', allure.attachment_type.TEXT)
            command_error = error_msg
            raise Exception(f"Binary deployment failed on node. Binary result: {binary_result}")

        else:
            with allure.step(
                f'Running workload on node {node.host} '
                f'with table type {table_type}'
            ):
                target_path = binary_result['path']
                cmd = (
                    f"{target_path} --endpoint {YdbCluster.ydb_endpoint} "
                    f"--database /{YdbCluster.ydb_database} "
                    f"--duration {self.timeout} --mode {table_type}"
                )
                allure.attach(cmd, 'Command to execute', allure.attachment_type.TEXT)
                LOGGER.info(f"Executing command on node {node.host}")

                try:
                    stdout, stderr = execute_command(
                        node.host, cmd, raise_on_error=False,
                        timeout=int(self.timeout * 2), raise_on_timeout=False)
                    command_result = stdout
                    command_error = stderr
                    LOGGER.info(f"Command executed successfully. STDOUT: {command_result}")
                    allure.attach(command_result, 'Workload stdout', allure.attachment_type.TEXT)
                    if command_error:
                        LOGGER.warning(f"Workload stderr: {command_error}")
                        allure.attach(command_error[:100], 'Workload stderr', allure.attachment_type.TEXT)

                except Exception as e:
                    error_msg = f"Command execution failed: {str(e)}"
                    LOGGER.error(error_msg)
                    allure.attach(error_msg, 'Workload execution error', allure.attachment_type.TEXT)
                    command_error = 'Error in workload, check logs:\n' + str(e)[:200] + '...'
                    raise Exception(f"Workload get errors in run: {command_error}")

        with allure.step('Checking scheme state'):
            stdout, stderr = execute_command(
                node.host,
                [YDB_CLI_PATH, '--endpoint', f'{YdbCluster.ydb_endpoint}',
                 '--database', f'/{YdbCluster.ydb_database}',
                 "scheme", "ls", "-lR"],
                raise_on_error=False)
            allure.attach(stdout, 'Scheme state stdout', allure.attachment_type.TEXT)
            if stderr:
                allure.attach(stderr, 'Scheme state stderr', allure.attachment_type.TEXT)
            LOGGER.info(f'stdout: {stdout}')
            if stderr:
                LOGGER.warning(f'stderr: {stderr}')
            LOGGER.info(f'path to check:{node.host.split(".")[0]}_0')

        result = YdbCliHelper.WorkloadRunResult()

        # Добавляем результаты выполнения команды
        if command_result is not None:
            result.stdout = str(command_result)
            # Проверяем на наличие ошибок в выводе
            if "error" in str(command_error).lower():
                result.add_error(str(command_error))
            # Проверяем на наличие предупреждений
            if ("warning: permanently added" not in str(command_error).lower() and
                    "warning" in str(command_error).lower()):
                result.add_warning(str(command_error))
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
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
