import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.cut_history import CUT_HISTORY_FLAGS, HIVE_CONFIG, CutHistoryScenario
from ydb.tests.oss.ydb_sdk_import import ydb


class TestCutHistory:
    @pytest.fixture(autouse=True)
    def setup(self):
        config = KikimrConfigGenerator(
            nodes=1,
            erasure=Erasure.NONE,
            extra_feature_flags=CUT_HISTORY_FLAGS,
            hive_config=HIVE_CONFIG,
        )
        self.cluster = KiKiMR(config)
        self.cluster.start()
        try:
            with ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database="/Root") as driver:
                driver.wait(timeout=60)
                self.driver = driver
                yield
        finally:
            self.cluster.stop()

    def test_empty_history_is_cut_and_live_data_survives(self):
        scenario = CutHistoryScenario(self.cluster)
        empty, occupied, rows = scenario.prepare(self.driver)
        scenario.restart()
        scenario.wait_for_cut(empty, occupied)
        scenario.assert_rows(self.driver, rows)
        scenario.restart()
        assert scenario.entries().isdisjoint(empty)
        scenario.assert_rows(self.driver, rows)
