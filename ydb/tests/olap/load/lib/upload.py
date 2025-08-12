from __future__ import annotations
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
from time import time, sleep
import yatest.common
import allure
import json
import logging
import ydb.tests.olap.lib.remote_execution as re
import ydb
import pytest
from .tpch import TpchSuiteBase


class UploadSuiteBase(LoadSuiteBase):
    query_name = 'Upload'
    upload_result: YdbCliHelper.WorkloadRunResult = None

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
    def validate(cls, result: YdbCliHelper.WorkloadRunResult):
        pass

    @classmethod
    def save_result_additional_info(cls, result: YdbCliHelper.WorkloadRunResult):
        pass

    @classmethod
    def do_setup_class(cls) -> None:
        start_time = time()
        result = YdbCliHelper.WorkloadRunResult()
        result.iterations[0] = YdbCliHelper.Iteration()
        result.traceback = None
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
            result.add_error(str(e))
            result.traceback = e.__traceback__
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
        result = sth.execute_scan_query(f'''
            SELECT COUNT(*)
            FROM `{table_full_path}/.sys/primary_index_optimizer_stats`
            WHERE CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.weight") AS Uint64) > 0
        ''')
        return result.result_set.rows[0][0] == 0

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
                return int(node.get('E-Rows', 0))
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
    def save_result_additional_info(cls, result: YdbCliHelper.WorkloadRunResult):
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
    __static_nodes: list[YdbCluster.Node] = []
    workload_type = TpchSuiteBase.workload_type
    iterations = TpchSuiteBase.iterations
    tables_size = TpchSuiteBase.tables_size
    check_canonical = TpchSuiteBase.check_canonical

    @classmethod
    def __execute_on_nodes(cls, cmd):
        execs = []
        for i in range(len(cls.__static_nodes)):
            node = cls.__static_nodes[i]
            execs.append(cls.execute_ssh(node.host, cmd))
        for e in execs:
            e.wait()

    @classmethod
    def get_path(cls):
        return f'upload/tpch/s{cls.scale}'

    @classmethod
    def get_remote_tmpdir(cls):
        for node in cls.__static_nodes:
            if re.is_localhost(node.host):
                return yatest.common.work_path(f'scripts/{cls.get_path()}')
        return f'/tmp/{cls.get_path()}'

    @classmethod
    def do_setup_class(cls) -> None:
        cls.__static_nodes = YdbCluster.get_cluster_nodes(role=YdbCluster.Node.Role.STORAGE, db_only=False)
        re.deploy_binaries_to_hosts(
            [YdbCliHelper.get_cli_path()],
            [n.host for n in cls.__static_nodes],
            cls.get_remote_tmpdir()
        )
        for i in range(len(cls.__static_nodes)):
            node = cls.__static_nodes[i]
            script_path = yatest.common.work_path('ydb_upload_tpch.sh')
            with open(script_path, 'w') as script_file:
                script_file.write(f'''#!/bin/bash

                    rm -f {cls.get_remote_tmpdir()}/state.json
                    RET_CODE=1
                    while [ $RET_CODE -ne 0 ]
                    do
                        {cls.get_remote_tmpdir()}/ydb -e grpc://{node.host}:{node.grpc_port} -d /{YdbCluster.ydb_database} \\
                            workload tpch -p {YdbCluster.get_tables_path(cls.get_path())} \\
                            import --bulk-size 50000 \\
                            generator --scale {cls.scale} -i {i} -C {len(cls.__static_nodes)} --state {cls.get_remote_tmpdir()}/state.json
                        RET_CODE="$?"
                    done
                ''')
            re.deploy_binary(script_path, node.host, cls.get_remote_tmpdir())
        super().do_setup_class()

    @classmethod
    def init(cls):
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'tpch', '-p', YdbCluster.get_tables_path(cls.get_path()), 'init', '--store=column', '--clear'])

    @classmethod
    def import_data(cls):
        cls.__execute_on_nodes(f'{cls.get_remote_tmpdir()}/ydb_upload_tpch.sh')

    @classmethod
    def do_teardown_class(cls) -> None:
        yatest.common.execute(YdbCliHelper.get_cli_command() + ['workload', 'tpch', '-p', YdbCluster.get_tables_path(cls.get_path()), 'clean'])
        cls.__execute_on_nodes(f'rm -rf {cls.get_remote_tmpdir()}')

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
