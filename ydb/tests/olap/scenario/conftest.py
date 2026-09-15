import logging
import inspect
import pytest
import time
import copy
from threading import Thread
from typing import List

from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import TestContext, ScenarioTestHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.allure_utils import allure_test_description
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


LOGGER = logging.getLogger()

SCENARIO_PREFIX = 'scenario_'


class YdbClusterInstance():
    '''
        Represents either long-running external cluster or create temporary cluster for local run
    '''
    _temp_ydb_cluster = None
    _endpoint = None
    _database = None
    _dyn_nodes_count = None

    def __init__(self, endpoint, database, config):
        if endpoint is not None:
            self._endpoint = endpoint
            self._database = database
            self._mon_port = 8765
        else:
            cluster = KiKiMR(configurator=config)
            cluster.start()
            node = cluster.nodes[1]
            self._endpoint = "grpc://%s:%d" % (node.host, node.port)
            self._database = config.domain_name
            self._mon_port = node.mon_port
            self._temp_ydb_cluster = cluster
            self._dyn_nodes_count = len(cluster.nodes)
        LOGGER.info(f'Using YDB, endpoint:{self._endpoint}, database:{self._database}')

    def endpoint(self):
        return self._endpoint

    def database(self):
        return self._database

    def dyn_nodes_count(self):
        return self._dyn_nodes_count

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
        cls._ydb_instance = YdbClusterInstance(ydb_endpoint, ydb_database, cls._get_cluster_config())
        YdbCluster.reset(cls._ydb_instance.endpoint(), cls._ydb_instance.database(), cls._ydb_instance.mon_port(), cls._ydb_instance.dyn_nodes_count())

    @classmethod
    def teardown_class(cls):
        cls._ydb_instance.stop()

    def worker(self, local_ctx: TestContext, suffix: str, codes: list, idx: int, errors: List[BaseException]):
        try:
            self._test_suffix(local_ctx, suffix, codes, idx)
        except BaseException as e:
            import sys
            print(f"Thread {idx} failed", file=sys.stderr)
            codes[idx] = 1
            errors.append(e)

    def test_multi(self, ctx: TestContext):
        if self.__class__.__name__ != 'TestInsert':
            return
        self.def_inserts_count = 50
        num_threads = int(get_external_param("num_threads", "5"))
        threads: List[Thread] = []
        exit_codes = [None] * num_threads
        errors: List[BaseException] = []

        for p in range(num_threads):
            suffix = str(p)
            t = Thread(target=self.worker, args=(copy.deepcopy(ctx), suffix, exit_codes, p, errors))

            try:
                t.start()
            except BaseException as e:
                exit_codes[p] = 1
                errors.append(e)
            else:
                threads.append(t)

        for t in threads:
            t.join()

        if errors:
            raise errors[0]

    def test(self, ctx: TestContext):
        self.def_inserts_count = 50
        exit_codes = [None]
        errors: List[BaseException] = []
        self.worker(ctx, get_external_param("table_suffix", ""), exit_codes, 0, errors)

        if errors:
            raise errors[0]

    def _test_suffix(self, ctx: TestContext, table_suffix: str, exit_codes, num: int):
        start_time = time.time()
        ctx.test += table_suffix
        test_path = ctx.test
        LOGGER.info('test_suffix, num {}, table path {} start_time {}'.format(num, test_path, start_time))
        ScenarioTestHelper(None).remove_path(test_path, ctx.suite)
        LOGGER.info('Path {} removed'.format(test_path))
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
            LOGGER.error('Caught skip exception, num {}'.format(num))
            allure_test_description(ctx.suite, ctx.test, start_time=start_time, end_time=time.time())
            raise
        except BaseException as e:
            LOGGER.error('Caught base exception, num {} message {}'.format(num, str(e)))
            ResultsProcessor.upload_results(
                kind='Scenario',
                suite=ctx.suite,
                test=ctx.test,
                timestamp=start_time,
                duration=time.time() - start_time,
                is_successful=False,
            )
            allure_test_description(ctx.suite, ctx.test, start_time=start_time, end_time=time.time())
            raise
        allure_test_description(ctx.suite, ctx.test, start_time=start_time, end_time=time.time())
        ScenarioTestHelper(None).remove_path(ctx.test, ctx.suite)
        LOGGER.info('Path {} removed'.format(ctx.test))
        exit_codes[num] = 0

    @classmethod
    def _get_cluster_config(cls):
        return KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_column_store": True,
                "enable_external_data_sources": True,
                "enable_tiering_in_column_shard": True,
            },
            column_shard_config={
                "generate_internal_path_id": True,
                "bulk_upsert_require_all_columns": False,
            },
            query_service_config=dict(
                available_external_data_sources=["ObjectStorage"]
            )
        )


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
