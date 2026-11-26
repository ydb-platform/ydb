from __future__ import annotations
from .conftest import LoadSuiteBase
from .tpch import TpchSuiteBase
from time import time, sleep
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.fill_tables import Dataset, TableFiller
from ydb.tests.olap.lib.workload_result import Iteration, WorkloadRunResult
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
import allure
import json
import logging
import pytest
import yatest.common
import ydb


class UploadSuiteBase(LoadSuiteBase):
    query_name = 'Upload'
    upload_result: WorkloadRunResult = None

    @classmethod
    def init(cls):
        pass

    @classmethod
    def import_data(cls):
        pass

    @classmethod
    def before_import_data(cls):
        pass

    @classmethod
    def after_import_data(cls):
        pass

    @classmethod
    def wait_compaction(cls):
        pass

    @classmethod
    def after_compaction(cls):
        pass

    @classmethod
    def validate(cls, result: WorkloadRunResult):
        pass

    @classmethod
    def save_result_additional_info(cls, result: WorkloadRunResult):
        pass

    @classmethod
    def do_setup_class(cls) -> None:
        start_time = time()
        result = WorkloadRunResult()
        result.iterations[0] = Iteration()
        nodes_start_time = [n.start_time for n in YdbCluster.get_cluster_nodes(db_only=False)]
        first_node_start_time = min(nodes_start_time) if len(nodes_start_time) > 0 else 0
        result.start_time = max(start_time - 600, first_node_start_time)
        try:
            cls.save_nodes_state()
            with allure.step("init"):
                cls.init()
            start_time = time()
            with allure.step("import data"):
                cls.before_import_data()
                cls.import_data()
                cls.after_import_data()
                cls.wait_compaction()
                cls.after_compaction()
        except BaseException as e:
            logging.error(f'Error: {e}')
            raise e
        result.iterations[0].time = time() - start_time
        cls.validate(result)
        cls.save_result_additional_info(result)
        cls.upload_result = result

    def test(self):
        if self.upload_result is None:
            raise RuntimeError("upload_result is None. Ensure do_setup_class() was called and completed successfully before running the test.")
        self.process_query_result(self.upload_result, self.query_name, True)


class UploadClusterBase(UploadSuiteBase):
    __saved_metrics: dict[str, dict[str, float]]
    __import_start_time = 0
    __import_time = 0
    __gross_time = 0

    @classmethod
    def get_path(cls) -> str:
        pass

    @classmethod
    def __get_metrics(cls) -> dict[str, dict[str, float]]:
        return YdbCluster.get_metrics(metrics={
            'written_bytes': {'Consumer': 'WRITING_OPERATOR', 'component': 'Writer', 'sensor': 'Deriviative/Requests/Bytes'},
            'compacted_bytes': {'Consumer': 'GENERAL_COMPACTION', 'component': 'Writer', 'sensor': 'Deriviative/Requests/Bytes'},
        })

    @classmethod
    def __compaction_complete_for_table(cls, table_full_path: str) -> bool:
        sth = ScenarioTestHelper(None)
        result = sth.execute_query(f'''
            SELECT COUNT(*)
            FROM `{table_full_path}/.sys/primary_index_optimizer_stats`
            WHERE CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.weight") AS Uint64) > 0
        ''')
        return result[0].rows[0][0] == 0

    @classmethod
    def __compaction_complete(cls) -> bool:
        for table in YdbCluster.get_tables(cls.get_path()):
            if not cls.__compaction_complete_for_table(table):
                return False
        return True

    @classmethod
    @allure.step
    def __stats_ready_for_table(cls,  table_full_path: str) -> bool:
        def __max_e_rows(node: dict):
            if node.get('Name') == 'TableFullScan' and node.get('Path') == table_full_path:
                return float(node.get('E-Rows', 0))
            children = [__max_e_rows(o) for o in node.get('Operators', [])] + [__max_e_rows(p) for p in node.get('Plans', [])]
            return max(children) if len(children) > 0 else 0

        driver: ydb.Driver = YdbCluster.get_ydb_driver()
        plan = json.loads(driver.table_client.session().create().explain(f'SELECT COUNT(*) FROM `{table_full_path}`').query_plan)
        return __max_e_rows(plan.get('Plan', {})) > 0

    @classmethod
    def __stats_ready(cls) -> bool:
        for table in YdbCluster.get_tables(cls.get_path()):
            if not cls.__stats_ready_for_table(table):
                return False
        return True

    @classmethod
    def __get_tables_size_bytes(cls) -> tuple[int, int]:
        sth = ScenarioTestHelper(None)
        raw_bytes = 0
        bytes = 0
        for table in YdbCluster.get_tables(cls.get_path()):
            table_raw_bytes, table_bytes = sth.get_volumes_columns(table, '')
            raw_bytes += table_raw_bytes
            bytes += table_bytes
        return raw_bytes, bytes

    @classmethod
    @allure.step
    def wait_compaction(cls):
        while not cls.__compaction_complete():
            sleep(1)

    @classmethod
    def before_import_data(cls):
        cls.__saved_metrics = cls.__get_metrics()
        cls.__import_start_time = time()

    @classmethod
    def after_import_data(cls):
        cls.__import_time = time() - cls.__import_start_time

    @classmethod
    def after_compaction(cls):
        cls.__gross_time = time() - cls.__import_start_time
        metrics = {}
        for slot, values in cls.__get_metrics().items():
            for k, v in values.items():
                metrics.setdefault(k, 0.)
                metrics[k] += v - cls.__saved_metrics.get(slot, {}).get(k, 0.)
        cls.__saved_metrics = metrics

    @classmethod
    def save_result_additional_info(cls, result: WorkloadRunResult):
        result.add_stat(cls.query_name, 'GrossTime', int(cls.__gross_time * 1000))
        result.add_stat(cls.query_name, 'time_with_compaction', int(cls.__gross_time * 1000))
        result.add_stat(cls.query_name, 'import_time', int(cls.__import_time * 1000))
        result.add_stat(cls.query_name, 'Mean', int(cls.__import_time * 1000))
        written_bytes = cls.__saved_metrics.get('written_bytes', 0.)
        compacted_bytes = cls.__saved_metrics.get('compacted_bytes', 0.)
        result.add_stat(cls.query_name, 'written_bytes', int(written_bytes))
        result.add_stat(cls.query_name, 'compacted_bytes', int(compacted_bytes))
        if written_bytes > 0.:
            result.add_stat(cls.query_name, 'write_amplification', compacted_bytes / written_bytes)
        raw_tables_size_bytes, tables_size_bytes = cls.__get_tables_size_bytes()
        result.add_stat(cls.query_name, 'tables_size_bytes', tables_size_bytes)
        result.add_stat(cls.query_name, 'raw_tables_size_bytes', raw_tables_size_bytes)
        with allure.step("wait tables statistics"):
            MAX_TIMEOUT = 1200
            start_wait = time()
            while not cls.__stats_ready():
                if time() - start_wait > MAX_TIMEOUT:
                    pytest.fail(f'Stats not ready before timeout {MAX_TIMEOUT}s')
                sleep(1)
            result.add_stat(cls.query_name, 'wait_stats_seconds', time() - start_wait)


class UploadTpchBase(UploadClusterBase):
    workload_type = TpchSuiteBase.workload_type
    iterations = TpchSuiteBase.iterations
    tables_size = TpchSuiteBase.tables_size
    check_canonical = TpchSuiteBase.check_canonical

    @classmethod
    def get_path(cls):
        return f'upload/tpch/s{cls.scale}'

    @classmethod
    def do_setup_class(cls) -> None:
        cls.dataset = Dataset('tpch', YdbCluster.get_tables_path(cls.get_path()),
                              scale=cls.scale, datetime_types='', clear=True,
                              import_options=('--bulk-size', '50000'))
        # Keep CLI deployment out of the measured import interval.
        with TableFiller(YdbCliHelper.get_cli_path(), key_file=YdbCluster.ydb_iam_file) as cls.filler:
            super().do_setup_class()

    @classmethod
    def init(cls):
        cls.filler.init(cls.dataset)

    @classmethod
    def import_data(cls):
        cls.filler.fill([cls.dataset], initialize=False)

    @classmethod
    def do_teardown_class(cls) -> None:
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'tpch', '-p', YdbCluster.get_tables_path(cls.get_path()), 'clean'])

    @pytest.mark.parametrize('query_num', [i for i in range(1, 23)])
    def test_tpch(self, query_num: int):
        self.run_workload_test(self.get_path(), query_num)


class TestUploadTpch1(UploadTpchBase):
    scale: int = 1


class TestUploadTpch10(UploadTpchBase):
    scale: int = 10


class TestUploadTpch100(UploadTpchBase):
    scale: int = 100


class TestUploadTpch1000(UploadTpchBase):
    scale: int = 1000
