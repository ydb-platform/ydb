import allure
import pytest
import os
import yatest
import logging
import time
from .conftest import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.remote_execution import execute_command, deploy_binaries_to_hosts
from ydb.tests.olap.lib.utils import get_external_param
from enum import Enum

LOGGER = logging.getLogger(__name__)


class TableType(str, Enum):
    """Тип таблицы"""
    ROW = 'row'
    COLUMN = 'column'


class SimpleQueueBase(WorkloadTestBase):
    # Настройки для базового класса
    workload_binary_name = 'simple_queue'
    workload_env_var = 'SIMPLE_QUEUE_BINARY'

    @pytest.mark.parametrize('table_type', [t.value for t in TableType])
    def test_workload_simple_queue(self, table_type: str, workload_executor):
        # Формируем аргументы команды
        command_args = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--duration {self.timeout} --mode {table_type}"
        )
        
        # Дополнительная статистика специфичная для SimpleQueue
        additional_stats = {
            "table_type": table_type,
            "workload_type": "simple_queue"
        }
        
        # Используем общий метод из базового класса
        self.execute_workload_test(
            workload_executor=workload_executor,
            workload_name=f"SimpleQueue_{table_type}",
            command_args=command_args,
            additional_stats=additional_stats
        )


class TestSimpleQueue(SimpleQueueBase):
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
