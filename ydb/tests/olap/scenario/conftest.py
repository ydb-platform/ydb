import logging
import inspect
import pytest
import time
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import TestContext, ScenarioTestHelper
from ydb.tests.olap.lib.utils import external_param_is_true
from ydb.tests.olap.lib.allure_utils import allure_test_description

LOGGER = logging.getLogger()

SCENARIO_PREFIX = 'scenario_'


class BaseTestSet:
    @classmethod
    def get_suite_name(cls):
        return cls.__name__

    @classmethod
    def setup_class(cls):
        ScenarioTestHelper(None).remove_path(cls.get_suite_name())

    @classmethod
    def teardown_class(cls):
        if not external_param_is_true('keep-tables'):
            ScenarioTestHelper(None).remove_path(cls.get_suite_name())

    def test(self, ctx: TestContext):
        allure_test_description(ctx.suite, ctx.test)
        start_time = time.time()
        try:
            ctx.executable(self, ctx)
            ResultsProcessor.upload_results(
                kind='Scenario',
                suite=ctx.suite,
                test=ctx.test,
                timestamp=start_time,
                duration=time.time() - start_time,
                is_successful=True,
            )
        except pytest.skip.Exception:
            raise
        except BaseException:
            ResultsProcessor.upload_results(
                kind='Scenario',
                suite=ctx.suite,
                test=ctx.test,
                timestamp=start_time,
                duration=time.time() - start_time,
                is_successful=False,
            )
            raise


def pytest_generate_tests(metafunc):
    idlist = []
    argvalues = []
    argnames = 'ctx'
    for method_name, scenario in sorted(
        inspect.getmembers(metafunc.cls, predicate=inspect.isfunction), key=lambda x: x[0]
    ):
        if not method_name.startswith(SCENARIO_PREFIX):
            continue
        ctx = TestContext(metafunc.cls.get_suite_name(), method_name[len(SCENARIO_PREFIX) :], scenario)
        idlist.append(ctx.test)
        argvalues.append(ctx)
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
