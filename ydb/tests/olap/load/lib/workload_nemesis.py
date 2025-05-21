import allure
import pytest
import os
import stat
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

from library.python import resource

STRESS_BINARIES_DEPLOY_PATH = '/tmp/stress_binaries/'
WORKLOAD_BINARY_NAME = 'simple_queue'  # Имя бинарного файла

class TestWorkloadNemesis(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'
    
    path = get_external_param('table-path-clickbench', f'{YdbCluster.tables_path}/clickbench/hits')

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
    
    def test_workload_simple_queue(self):
        """Тест запуска workload с простой очередью на всех нодах кластера"""
        # Распаковываем бинарный файл из ресурсов
        binary_path = self._unpack_workload_binary(WORKLOAD_BINARY_NAME)
        
        # Разворачиваем бинарный файл на всех нодах кластера
        deploy_results = YdbCluster.deploy_binaries_to_nodes([binary_path], STRESS_BINARIES_DEPLOY_PATH)
        
        # Для каждой ноды в кластере
        for node in YdbCluster.get_cluster_nodes():
            node_host = node.host
            print(f"Node: {node_host}")
            
            # Проверяем, успешно ли был развернут бинарный файл
            binary_result = deploy_results.get(node_host, {}).get(WORKLOAD_BINARY_NAME, {})
            success = binary_result.get('success', False)
            print(f"Binary status: {success}")
            
            # Запускаем бинарный файл на ноде, если он был успешно развернут
            if success:
                target_path = binary_result['path']
                cmd = f"{target_path} --endpoint {node.host}:{node.ic_port} --database {YdbCluster.ydb_database} --mode row"
                
                result = node.execute_command(cmd, raise_on_error=False)
                print(f"Execution result: {result}")
               
        # Сохраняем состояние нод и выполняем тестовый запрос
        self.save_nodes_state()
        result = YdbCliHelper.workload_run(
            path='path_1',
            query_name='query_name',
            iterations=1,
            workload_type=self.workload_type,
            timeout='qparams.timeout',
            check_canonical=self.check_canonical,
            query_syntax=self.query_syntax,
            scale=self.scale,
            query_prefix='qparams.query_prefix',
            external_path=self.get_external_path(),
        )
        self.process_query_result(result, 'ls', False)
