from __future__ import annotations
import pytest
import allure
import json
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, WorkloadType
from ydb.tests.olap.lib.allure_utils import allure_test_description
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from time import time
from typing import Optional
from allure_commons._core import plugin_manager
from allure_pytest.listener import AllureListener


class LoadSuiteBase:
    class QuerySettings:
        def __init__(self, iterations: Optional[int] = None, timeout: Optional[float] = None) -> None:
            self.iterations = iterations
            self.timeout = timeout

    iterations: int = 5
    workload_type: WorkloadType = None
    timeout: float = 1800.
    refference: str = ''
    check_canonical: bool = False
    query_settings: dict[int, LoadSuiteBase.QuerySettings] = {}

    @property
    def suite(self) -> str:
        result = type(self).__name__
        if result.startswith('Test'):
            return result[4:]
        return result

    @classmethod
    def _get_iterations(cls, query_num: int) -> int:
        q = cls.query_settings.get(query_num, None)
        return q.iterations if q is not None and q.iterations is not None else cls.iterations

    @classmethod
    def _get_timeout(cls, query_num: int) -> float:
        q = cls.query_settings.get(query_num, None)
        return q.timeout if q is not None and q.timeout is not None else cls.timeout

    @classmethod
    def _test_name(cls, query_num: int) -> str:
        return f'Query{query_num:02d}'

    def process_query_result(self, result: YdbCliHelper.WorkloadRunResult, query_num: int, iterations: int, upload: bool):
        def _get_duraton(stats, field):
            if stats is None:
                return None
            result = stats.get(field)
            return float(result) / 1e3 if result is not None else None

        def _attach_plans(plan: YdbCliHelper.QueryPlan) -> None:
            if plan.plan is not None:
                allure.attach(json.dumps(plan.plan), 'Plan json', attachment_type=allure.attachment_type.JSON)
            if plan.table is not None:
                allure.attach(plan.table, 'Plan table', attachment_type=allure.attachment_type.TEXT)
            if plan.ast is not None:
                allure.attach(plan.ast, 'Plan ast', attachment_type=allure.attachment_type.TEXT)
            if plan.svg is not None:
                allure.attach(plan.svg, 'Plan svg', attachment_type=allure.attachment_type.SVG)

        test = self._test_name(query_num)
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
            for i in range(self._get_iterations(query_num)):
                try:
                    with allure.step(f'Iteration {i}'):
                        _attach_plans(result.plans[i])
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
                value = int(stats[p])
                s = f'{int(value / 1000)}s ' if value >= 1000 else ''
                allure.dynamic.parameter(p, f'{s}{value % 1000}ms')
        error_message = ''
        success = True
        if not result.success:
            success = False
            error_message = result.error_message
        elif stats.get('FailsCount', 0) != 0:
            success = False
            error_message = 'There are fail attemps'
        if upload:
            ResultsProcessor.upload_results(
                kind='Load',
                suite=self.suite,
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
            exc = pytest.fail.Exception(error_message)
            if result.traceback is not None:
                exc = exc.with_traceback(result.traceback)
            raise exc

    def run_workload_test(self, path: str, query_num: int) -> None:
        allure_listener = next(filter(lambda x: isinstance(x, AllureListener), plugin_manager.get_plugin_manager().get_plugins()))
        allure_test_result = allure_listener.allure_logger.get_test(None)
        query_num_param = next(filter(lambda x: x.name == 'query_num', allure_test_result.parameters), None)
        if query_num_param:
            query_num_param.mode = allure.parameter_mode.HIDDEN.value
        start_time = time()
        result = YdbCliHelper.workload_run(
            path=path,
            query_num=query_num,
            iterations=self._get_iterations(query_num),
            workload_type=self.workload_type,
            timeout=self._get_timeout(query_num),
            check_canonical=self.check_canonical
        )
        allure_test_description(self.suite, self._test_name(query_num), refference_set=self.refference, start_time=start_time, end_time=time())
        self.process_query_result(result, query_num, self.iterations, True)
