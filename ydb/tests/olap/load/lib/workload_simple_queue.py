import pytest
from .workload_executor import WorkloadTestBase
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
    def test_workload_simple_queue(self, table_type: str):
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

        # Запускаем тест с чанками
        self.execute_workload_test(
            workload_name=f"SimpleQueue_{table_type}",
            command_args=command_args_template,
            duration_value=self.timeout,
            additional_stats=additional_stats,
            use_chunks=True,
            duration_param="--duration"
        )


class TestSimpleQueue(SimpleQueueBase):
    """Тест с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100))
