import allure
import pytest
from .conftest import LoadSuiteBase
from os import getenv, os, stat
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param

from library.python import resource

STRESS_BINARIES_DEPLOY_PATH = '/tmp/stress_binaries/'
WORKLOAD_BINARY_PATH = os.path.join(STRESS_BINARIES_DEPLOY_PATH, WORKLOAD_BINARY_NAME)

class TestWorkloadNemesis(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'
    
    path = get_external_param('table-path-clickbench', f'{YdbCluster.tables_path}/clickbench/hits')

    def deploy_tools(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(["sudo", "mkdir", "-p", STRESS_BINARIES_DEPLOY_PATH], raise_on_error=False)
            for artifact in self.artifacts:
                node_artifact_path = os.path.join(
                    STRESS_BINARIES_DEPLOY_PATH,
                    os.path.basename(
                        artifact
                    )
                )
                node.copy_file_or_dir(
                    artifact,
                    node_artifact_path
                )
                node.ssh_command(f"sudo chmod 777 {node_artifact_path}", raise_on_error=False)

    def _unpack_resource(self, name):
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        return path_to_unpack
    
    def _unpack_workload_binary(self, workload_binary_name: str):
        return self._unpack_resource(workload_binary_name)
    
    def test_workload_simple_queue(self):
        print('SELFDATA')
        print(dir(self))
        print('CLUSTERDATA')
        print(dir(YdbCluster))
        for  node in YdbCluster.get_cluster_nodes():
                #self.execute( f'sudo chmod 777 /home/kirrysin/fork2/ydb/tests/stress/simple_queue/simple_queue --endpoint {node.host}:{node.ic_port}  --database {YdbCluster.ydb_database} --mode row ')
                print('NODEDATA')
                print(dir(node))
                result = self.execute( f'pwd')
                print(result)
                result = self.execute( f'ls /tmp/simple_queue/ -lhtr')
                print(result)
                #result = self.execute( f'/tmp/simple_queue/simple_queue --endpoint {node.host}:{node.ic_port}  --database {YdbCluster.ydb_database} --mode row ')
                #print(result)
   
               
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
       # self.run_workload_test(self.path, 12)
