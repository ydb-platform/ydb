import os
import ydb.tests.olap.load.lib.clickbench as clickbench
import yatest.common
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestClickbench(clickbench.TestClickbench, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        os.environ['YDB_HARD_MEMORY_LIMIT_BYTES'] = '107374182400'
        cls.setup_cluster()
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column'])
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        os.environ['NO_VERIFY_DATA_CLICKBENCH'] = '1'
        clickbench.TestClickbench.setup_class()
