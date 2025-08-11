from ydb.tests.olap.load.lib.tpch import TestTpch1 as Tpch1
from ydb.tests.olap.load.lib.tpcds import TestTpcds1 as Tpcds1
from ydb.tests.olap.load.lib.clickbench import TestClickbench as Clickbench
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cli import CheckCanonicalPolicy
import pytest


EXPECTED_ERRORS = {
    CheckCanonicalPolicy.NO: None.__class__,
    CheckCanonicalPolicy.WARNING: Exception,
    CheckCanonicalPolicy.ERROR: pytest.fail.Exception,
}


class TestTpchDiffProcessing(Tpch1, FunctionalTestBase):
    iterations: int = 1
    verify_data = False

    @pytest.mark.parametrize('policy', EXPECTED_ERRORS.keys())
    def test_tpch(self, policy):
        self.check_canonical = policy
        exc = None
        try:
            Tpch1.test_tpch(self, 1)
        except BaseException as e:
            exc = e
        assert exc.__class__ == EXPECTED_ERRORS[policy]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s1', 'init', '--store=column'])
        super().setup_class()


class TestTpcdsDiffProcessing(Tpcds1, FunctionalTestBase):
    iterations: int = 1
    verify_data = False

    @pytest.mark.parametrize('policy', EXPECTED_ERRORS.keys())
    def test_tpcds(self, policy):
        self.check_canonical = policy
        exc = None
        try:
            Tpcds1.test_tpcds(self, 1)
        except BaseException as e:
            exc = e
        assert exc.__class__ == EXPECTED_ERRORS[policy]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpcds', '-p', 'olap_yatests/tpcds/s1', 'init', '--store=column'])
        super().setup_class()


class TestClickbenchDiffProcessing(Clickbench, FunctionalTestBase):
    iterations: int = 1
    verify_data = False

    @pytest.mark.parametrize('policy', EXPECTED_ERRORS.keys())
    def test_clickbench(self, policy):
        self.check_canonical = policy
        exc = None
        try:
            Clickbench.test_clickbench(self, "Query01")
        except BaseException as e:
            exc = e
        assert exc.__class__ == EXPECTED_ERRORS[policy]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column'])
        super().setup_class()
