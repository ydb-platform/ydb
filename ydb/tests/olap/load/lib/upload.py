from __future__ import annotations
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from time import time
import yatest.common
import allure
import logging


class UploadSuiteBase(LoadSuiteBase):
    query_name = 'Upload'

    def init(self):
        pass

    def import_data(self):
        pass

    def validate(self, result: YdbCliHelper.WorkloadRunResult):
        pass

    def save_result_additional_info(self, result: YdbCliHelper.WorkloadRunResult):
        pass

    def test(self):
        start_time = time()
        result = YdbCliHelper.WorkloadRunResult()
        result.iterations[0] = YdbCliHelper.Iteration()
        result.traceback = None
        nodes_start_time = [n.start_time for n in YdbCluster.get_cluster_nodes(db_only=False)]
        first_node_start_time = min(nodes_start_time) if len(nodes_start_time) > 0 else 0
        result.start_time = max(start_time - 600, first_node_start_time)
        try:
            self.save_nodes_state()
            with allure.step("init"):
                self.init()
            start_time = time()
            with allure.step("import data"):
                self.import_data()
        except BaseException as e:
            logging.error(f'Error: {e}')
            result.add_error(str(e))
            result.traceback = e.__traceback__
        result.iterations[0].time = time() - start_time
        self.validate(result)
        self.save_result_additional_info(result)
        self.process_query_result(result, self.query_name, True)


class UploadTpchBase(UploadSuiteBase):
    @classmethod
    def __get_path(cls):
        return YdbCluster.get_tables_path(f'upload/tpch/s{cls.scale}')

    def init(self):
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'tpch', '-p', self.__get_path(), 'init', '--store=column'])

    def import_data(self):
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'tpch', '-p', self.__get_path(), 'import', 'generator', '--scale', str(self.scale)])


class TestUploadTpch1(UploadTpchBase):
    scale: int = 1
