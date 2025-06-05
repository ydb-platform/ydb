import pytest
from .conftest import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import Enum

import logging
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
        # Формируем аргументы команды (без --duration, он будет добавлен в чанках)
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--mode {table_type}"
        )
        
        # Дополнительная статистика специфичная для SimpleQueue
        additional_stats = {
            "table_type": table_type,
            "workload_type": "simple_queue"
        }
        
        # Используем новый метод с чанками для повышения надежности
        self.execute_workload_test_with_chunks(
            workload_executor=workload_executor,
            workload_name=f"SimpleQueue_{table_type}",
            command_args_template=command_args_template,
            additional_stats=additional_stats
        )


class TestSimpleQueue(SimpleQueueBase):
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
