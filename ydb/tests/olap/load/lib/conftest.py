from __future__ import annotations
import pytest
import allure
import json
import yatest
import os
import logging
from allure_commons._core import plugin_manager
from allure_pytest.listener import AllureListener
from copy import deepcopy
from datetime import datetime
from pytz import timezone
from time import time
from typing import Optional
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.allure_utils import allure_test_description
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper


class LoadSuiteBase:
    class QuerySettings:
        def __init__(self, iterations: Optional[int] = None, timeout: Optional[float] = None, query_prefix: Optional[str] = None) -> None:
            self.iterations = iterations
            self.timeout = timeout
            self.query_prefix = query_prefix

    iterations: int = 5
    workload_type: WorkloadType = None
    timeout: float = 1800.
    refference: str = ''
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.NO
    query_syntax: str = ''
    query_settings: dict[int, LoadSuiteBase.QuerySettings] = {}
    scale: Optional[int] = None
    query_prefix: str = get_external_param('query-prefix', '')

    @classmethod
    def suite(cls) -> str:
        result = cls.__name__
        if result.startswith('Test'):
            return result[4:]
        return result

    @classmethod
    def _get_query_settings(cls, query_num: int) -> QuerySettings:
        result = LoadSuiteBase.QuerySettings(
            iterations=cls.iterations,
            timeout=cls.timeout,
            query_prefix=cls.query_prefix
        )
        q = cls.query_settings.get(query_num, LoadSuiteBase.QuerySettings())
        if q.iterations is not None:
            result.iterations = q.iterations
        if q.timeout is not None:
            result.timeout = q.timeout
        if q.query_prefix is not None:
            result.query_prefix = q.query_prefix
        return result

    @classmethod
    def _test_name(cls, query_num: int) -> str:
        return f'Query{query_num:02d}'

    @classmethod
    @allure.step('check tables size')
    def check_tables_size(cls, folder: Optional[str], tables: dict[str, int]):
        wait_error = YdbCluster.wait_ydb_alive(
            20 * 60, (
                f'{YdbCluster.tables_path}/{folder}'
                if folder is not None
                else [f'{YdbCluster.tables_path}/{t}' for t in tables.keys()]
            ))
        if wait_error is not None:
            pytest.fail(f'Cluster is dead: {wait_error}')
        sth = ScenarioTestHelper(None)
        errors: list[str] = []
        for table, expected_size in tables.items():
            if folder is None:
                table_full = table
            elif folder.endswith('/') or table.startswith('/'):
                table_full = f'{folder}{table}'
            else:
                table_full = f'{folder}/{table}'
            size = sth.get_table_rows_count(table_full)
            if size != expected_size:
                errors.append(f'table `{table}`: expect {expected_size}, but actually is {size};')
        if len(errors) > 0:
            msg = "\n".join(errors)
            pytest.fail(f'Unexpected tables size in `{folder}`:\n {msg}')

    @classmethod
    def _attach_logs(cls, start_time, attach_name):
        hosts = [node.host for node in filter(lambda x: x.role == YdbCluster.Node.Role.STORAGE, YdbCluster.get_cluster_nodes())]
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        cmd = f"ulimit -n 100500;unified_agent select -S '{start}' -s {{storage}} -m k8s_container:{{container}}"
        exec_kikimr = {
            'ydb-dynamic': {},
            'ydb-storage': {},
        }
        exec_start = deepcopy(exec_kikimr)
        ssh_cmd = ['ssh', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
        ssh_user = os.getenv('SSH_USER')
        if ssh_user is not None:
            ssh_cmd += ['-l', ssh_user]
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file is not None:
            ssh_cmd += ['-i', ssh_key_file]
        for host in hosts:
            for c in exec_kikimr.keys():
                try:
                    exec_kikimr[c][host] = yatest.common.execute(ssh_cmd + [host, cmd.format(storage='kikimr', container=c)], wait=False)
                except BaseException as e:
                    logging.error(e)
            for c in exec_start.keys():
                try:
                    exec_start[c][host] = yatest.common.execute(ssh_cmd + [host, cmd.format(storage='kikimr-start', container=c)], wait=False)
                except BaseException as e:
                    logging.error(e)

        error_log = ''
        for c, execs in exec_start.items():
            for host, e in sorted(execs.items()):
                e.wait(check_exit_code=False)
                error_log += f'{host}:\n'
                error_log += (e.stdout if e.returncode == 0 else e.stderr).decode('utf-8') + '\n'
            allure.attach(error_log, f'{attach_name}_{c}_stderr', allure.attachment_type.TEXT)

        for c, execs in exec_kikimr.items():
            dir = os.path.join(yatest.common.tempfile.gettempdir(), f'{attach_name}_{c}_logs')
            os.makedirs(dir, exist_ok=True)
            for host, e in execs.items():
                e.wait(check_exit_code=False)
                with open(os.path.join(dir, host), 'w') as f:
                    f.write((e.stdout if e.returncode == 0 else e.stderr).decode('utf-8'))
            archive = dir + '.tar.gz'
            yatest.common.execute(['tar', '-C', dir, '-czf', archive, '.'])
            allure.attach.file(archive, f'{attach_name}_{c}_logs', extension='tar.gz')

    @classmethod
    def process_query_result(cls, result: YdbCliHelper.WorkloadRunResult, query_num: int, iterations: int, upload: bool):
        def _get_duraton(stats, field):
            if stats is None:
                return None
            result = stats.get(field)
            return float(result) / 1e3 if result is not None else None

        def _duration_text(duration: float | int):
            s = f'{int(duration)}s ' if duration >= 1 else ''
            return f'{s}{int(duration * 1000) % 1000}ms'

        def _attach_plans(plan: YdbCliHelper.QueryPlan) -> None:
            if plan.plan is not None:
                allure.attach(json.dumps(plan.plan), 'Plan json', attachment_type=allure.attachment_type.JSON)
            if plan.table is not None:
                allure.attach(plan.table, 'Plan table', attachment_type=allure.attachment_type.TEXT)
            if plan.ast is not None:
                allure.attach(plan.ast, 'Plan ast', attachment_type=allure.attachment_type.TEXT)
            if plan.svg is not None:
                allure.attach(plan.svg, 'Plan svg', attachment_type=allure.attachment_type.SVG)

        test = cls._test_name(query_num)
        stats = result.stats.get(test)
        if stats is not None:
            allure.attach(json.dumps(stats, indent=2), 'Stats', attachment_type=allure.attachment_type.JSON)
        else:
            stats = {}
        if result.query_out is not None:
            allure.attach(result.query_out, 'Query output', attachment_type=allure.attachment_type.TEXT)

        if result.explain_plan is not None:
            with allure.step('Explain'):
                _attach_plans(result.explain_plan)

        if result.plans is not None:
            for i in range(iterations):
                s = allure.step(f'Iteration {i}')
                if i in result.time_by_iter:
                    s.params['duration'] = _duration_text(result.time_by_iter[i])
                try:
                    with s:
                        _attach_plans(result.plans[i])
                        if i in result.time_by_iter:
                            allure.dynamic.parameter('duration', _duration_text(result.time_by_iter[i]))
                        if i in result.errors_by_iter:
                            pytest.fail(result.errors_by_iter[i])
                except BaseException:
                    pass

        if result.stdout is not None:
            allure.attach(result.stdout, 'Stdout', attachment_type=allure.attachment_type.TEXT)
            begin_text = 'Query text:\n'
            begin_pos = result.stdout.find(begin_text)
            if begin_pos >= 0:
                begin_pos += len(begin_text)
                end_pos = result.stdout.find("\n\n\titeration")
                if end_pos < 0:
                    end_pos = len(result.stdout)
                query_text = result.stdout[begin_pos:end_pos]
                allure.attach(query_text, 'Query text', attachment_type=allure.attachment_type.TEXT)

        if result.stderr is not None:
            allure.attach(result.stderr, 'Stderr', attachment_type=allure.attachment_type.TEXT)
        for p in ['Mean']:
            if p in stats:
                allure.dynamic.parameter(p, _duration_text(stats[p] / 1000.))
        error_message = ''
        success = True
        if not result.success:
            success = False
            error_message = result.error_message
        elif stats.get('FailsCount', 0) != 0:
            success = False
            error_message = 'There are fail attemps'
        if os.getenv('NO_KUBER_LOGS') is None and not success:
            cls._attach_logs(start_time=result.start_time, attach_name='kikimr')
        if upload:
            ResultsProcessor.upload_results(
                kind='Load',
                suite=cls.suite(),
                test=test,
                timestamp=time(),
                is_successful=success,
                min_duration=_get_duraton(stats, 'Min'),
                max_duration=_get_duraton(stats, 'Max'),
                mean_duration=_get_duraton(stats, 'Mean'),
                median_duration=_get_duraton(stats, 'Median'),
                statistics=stats,
            )
        if not success:
            exc = pytest.fail.Exception('\n'.join([error_message, result.warning_message]))
            if result.traceback is not None:
                exc = exc.with_traceback(result.traceback)
            raise exc
        if result.warning_message:
            raise Exception(result.warning_message)

    @classmethod
    def setup_class(cls) -> None:
        start_time = time()
        error = YdbCluster.wait_ydb_alive(20 * 60)
        tb = None
        if not error and hasattr(cls, 'do_setup_class'):
            try:
                cls.do_setup_class()
            except BaseException as e:
                error = str(e)
                tb = e.__traceback__
        first_node_start_time = min([n.start_time for n in YdbCluster.get_cluster_nodes(db_only=False)])
        ResultsProcessor.upload_results(
            kind='Load',
            suite=cls.suite(),
            test='_Verification',
            timestamp=start_time,
            is_successful=(error is None)
        )
        if os.getenv('NO_KUBER_LOGS') is None and error is not None:
            cls._attach_logs(start_time=max(start_time - 600, first_node_start_time), attach_name='kikimr_start')
        if error is not None:
            exc = pytest.fail.Exception(error)
            exc.with_traceback(tb)
            raise exc

    def run_workload_test(self, path: str, query_num: int) -> None:
        for plugin in plugin_manager.get_plugin_manager().get_plugins():
            if isinstance(plugin, AllureListener):
                allure_test_result = plugin.allure_logger.get_test(None)
                if allure_test_result is not None:
                    for param in allure_test_result.parameters:
                        if param.name == 'query_num':
                            param.mode = allure.parameter_mode.HIDDEN.value
        qparams = self._get_query_settings(query_num)
        result = YdbCliHelper.workload_run(
            path=path,
            query_num=query_num,
            iterations=qparams.iterations,
            workload_type=self.workload_type,
            timeout=qparams.timeout,
            check_canonical=self.check_canonical,
            query_syntax=self.query_syntax,
            scale=self.scale,
            query_prefix=qparams.query_prefix
        )
        allure_test_description(self.suite(), self._test_name(query_num), refference_set=self.refference, start_time=result.start_time, end_time=time())
        self.process_query_result(result, query_num, qparams.iterations, True)
