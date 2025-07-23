import yatest.common
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestDefaultPath(FunctionalTestBase):
    def test_clickbench(self):
        self.run_cli(['workload', 'clickbench', 'init'])
        self.run_cli(['workload', 'clickbench', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        self.run_cli(['workload', 'clickbench', 'run', '--include', '1'])
        self.run_cli(['workload', 'clickbench', 'clean'])

    def test_tpcds(self):
        self.run_cli(['workload', 'tpcds', 'init'])
        self.run_cli(['workload', 'tpcds', 'import', 'generator', '--scale=0.1'])
        self.run_cli(['workload', 'tpcds', 'run', '--scale=0.1', '--include', '1'])
        self.run_cli(['workload', 'tpcds', 'clean'])

    def test_tpch(self):
        self.run_cli(['workload', 'tpch', 'init'])
        self.run_cli(['workload', 'tpch', 'import', 'generator', '--scale=0.1'])
        self.run_cli(['workload', 'tpch', 'run', '--scale=0.1', '--include', '1'])
        self.run_cli(['workload', 'tpch', 'clean'])

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
