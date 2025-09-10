import ydb.tests.olap.load.lib.workload_manager as wm
import yatest.common
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestClickbenchWM(wm.TestWorkloadMangerClickbenchConcurentQueryLimit, FunctionalTestBase):
    iterations: int = 2
    verify_data: bool = False

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        super().setup_class()
