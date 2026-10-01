from ydb.tests.olap.load.lib.tpch import TestTpch1 as Tpch1
from ydb.tests.olap.load.lib.tpcds import TestTpcds1 as Tpcds1
from ydb.tests.olap.load.lib.clickbench import TestClickbench as Clickbench
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cli import CheckCanonicalPolicy, YdbCliHelper
import csv
import pytest
import yatest.common


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


class TestQueryCanonicalComparison(FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()

    @classmethod
    def teardown_class(cls) -> None:
        if cls.cluster is not None:
            cls.cluster.stop()

    @pytest.mark.parametrize('expression, expected, diffs_count', [
        pytest.param('""u', '', 0, id='utf8-empty'),
        pytest.param('""', '', 0, id='string-empty'),
        pytest.param('Just(""u)', '', 0, id='optional-utf8-empty'),
        pytest.param('Just(""u)', '""', 0, id='optional-utf8-quoted-empty'),
        pytest.param('Just("")', '', 0, id='optional-string-empty'),
        pytest.param('Just("")', '""', 0, id='optional-string-quoted-empty'),
        pytest.param('Just("hello"u)', 'hello', 0, id='optional-utf8-nonempty'),
        pytest.param('Just("hello")', 'hello', 0, id='optional-string-nonempty'),
        pytest.param('Nothing(Utf8?)', '', 0, id='null-utf8'),
        pytest.param('Nothing(String?)', '', 0, id='null-string'),
        pytest.param('Just("hello"u)', '', 1, id='nonempty-vs-empty'),
        pytest.param('Just(""u)', 'hello', 1, id='empty-vs-nonempty'),
        pytest.param('Nothing(Utf8?)', 'hello', 1, id='null-vs-nonempty'),
        pytest.param('Just(0u)', '', 1, id='zero-vs-empty'),
        pytest.param('Just(false)', '', 1, id='false-vs-empty'),
        pytest.param('Just(Decimal("0", 22, 9))', '', 1, id='decimal-vs-empty'),
    ])
    def test_compare_value(self, tmp_path, expression, expected, diffs_count):
        run_path = tmp_path / 'run'
        run_path.mkdir()
        (run_path / 'query.sql').write_text(f'SELECT 1 AS id, {expression} AS value;\n')
        # Keep a second column so an empty value does not become an empty CSV line.
        (run_path / 'query.sql.result').write_text(f'id,value\n1,{expected}\n')
        report_path = tmp_path / 'report.csv'
        result = yatest.common.execute(YdbCliHelper.get_cli_command() + [
            'workload', 'query', 'run', '--suite-path', str(tmp_path),
            '--check-canonical', '--iterations', '1',
            '--csv', str(report_path), '--output', str(tmp_path / 'results'),
        ], check_exit_code=False)

        with report_path.open() as report:
            rows = list(csv.DictReader(report))
        query_rows = [row for row in rows if row['Query #'] == 'query.sql']
        assert len(query_rows) == 1, rows
        row = query_rows[0]
        assert int(row['SuccessCount'] or 0) == 1, result.std_err
        assert int(row['FailsCount'] or 0) == 0, result.std_err
        assert int(row['DiffsCount'] or 0) == diffs_count, result.std_err
        assert result.exit_code == (1 if diffs_count else 0), result.std_err
