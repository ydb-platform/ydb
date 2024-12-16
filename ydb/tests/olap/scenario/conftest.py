import logging
import inspect
import pytest
import time
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import TestContext, ScenarioTestHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import external_param_is_true
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.allure_utils import allure_test_description
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


LOGGER = logging.getLogger()

SCENARIO_PREFIX = 'scenario_'


class YdbClusterInstance():
    '''
        Represents either long-running external cluster or create temporary cluster for local run
    '''
    _temp_ydb_cluster = None
    _endpoint = None
    _database = None

    def __init__(self, endpoint, database):
        if endpoint is not None:
            self._endpoint = endpoint
            self._database = database
            self._mon_port = 8765
        else:
            config = KikimrConfigGenerator()
            cluster = KiKiMR(configurator=config)
            cluster.start()
            node = cluster.nodes[1]
            self._endpoint = "grpc://%s:%d" % (node.host, node.port)
            self._database = config.domain_name
            self._mon_port = node.mon_port
            self._temp_ydb_cluster = cluster
        LOGGER.info(f'Using YDB, endpoint:{self._endpoint}, database:{self._database}')

    def endpoint(self):
        return self._endpoint

    def database(self):
        return self._database

    def mon_port(self):
        return self._mon_port

    def stop(self):
        if self._temp_ydb_cluster is not None:
            self._temp_ydb_cluster.stop()
        self._temp_ydb_cluster = None


class BaseTestSet:
    @classmethod
    def get_suite_name(cls):
        return cls.__name__

    @classmethod
    def setup_class(cls):
        ydb_endpoint = get_external_param('ydb-endpoint', None)
        ydb_database = get_external_param('ydb-db', "").lstrip('/')
        cls._ydb_instance = YdbClusterInstance(ydb_endpoint, ydb_database)
        YdbCluster.reset(cls._ydb_instance.endpoint(), cls._ydb_instance.database(), cls._ydb_instance.mon_port())
        if not external_param_is_true('reuse-tables'):
            ScenarioTestHelper(None).remove_path(cls.get_suite_name())

    @classmethod
    def teardown_class(cls):
        if not external_param_is_true('keep-tables'):
            ScenarioTestHelper(None).remove_path(cls.get_suite_name())
        cls._ydb_instance.stop()

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
