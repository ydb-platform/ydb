import pytest
from .conftest import WorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

import logging
LOGGER = logging.getLogger(__name__)


class OltpWorkloadBase(WorkloadTestBase):
    # Настройки для базового класса
    workload_binary_name = 'oltp_workload'
    workload_env_var = 'OLTP_WORKLOAD_BINARY'

    def test_workload_oltp(self, workload_executor):
        # Формируем аргументы команды для OLTP workload (без --duration, он будет добавлен в чанках)
        command_args_template = (
            f"--endpoint {YdbCluster.ydb_endpoint} "
            f"--database /{YdbCluster.ydb_database} "
            f"--path oltp_workload"
        )
        
        # Дополнительная статистика специфичная для OLTP
        additional_stats = {
            "workload_type": "oltp",
            "path": "oltp_workload"
        }
        
        # Используем новый метод с чанками для повышения надежности
        self.execute_workload_test_with_chunks(
            workload_executor=workload_executor,
            workload_name="OltpWorkload",
            command_args_template=command_args_template,
            duration_param="--duration",
            duration_value=self.timeout,
            additional_stats=additional_stats
        )


class TestOltpWorkload(OltpWorkloadBase):
    """Тест OLTP workload с таймаутом из get_external_param"""
    timeout = int(get_external_param('workload_duration', 100)) 