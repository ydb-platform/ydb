import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    current_binary_path,
    inter_stable_binary_path,
)
from ydb.tests.olap.common.cut_history import CUT_HISTORY_FLAGS, HIVE_CONFIG, CutHistoryScenario


class TestCutHistoryDormant(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(nodes=3, erasure=Erasure.NONE, table_service_config={"enable_olap_sink": True})

    def test_data_and_history_survive_roll(self):
        scenario = CutHistoryScenario(self.cluster)
        empty, occupied, rows = scenario.prepare(self.driver)
        for _ in self.roll():
            rows.update(scenario.write_rows(self.driver, offset=len(rows)))
            scenario.assert_rows(self.driver, rows)
            assert empty | occupied <= scenario.entries()


@pytest.mark.parametrize(
    "base_setup",
    [[inter_stable_binary_path, current_binary_path]],
    indirect=True,
    ids=["stable_to_current_and_back"],
)
class TestCutHistoryUpgradeAndRollback(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(
            nodes=1, erasure=Erasure.NONE, hive_config=HIVE_CONFIG, table_service_config={"enable_olap_sink": True}
        )

    def test_cut_survives_rollback(self):
        scenario = CutHistoryScenario(self.cluster)
        empty, occupied, rows = scenario.prepare(self.driver)
        assert empty | occupied <= scenario.entries()

        flags = self.config.yaml_config["feature_flags"]
        for flag in CUT_HISTORY_FLAGS:
            flags[flag] = True
        self.change_cluster_version()
        scenario.wait_for_cut(empty, occupied)
        scenario.assert_rows(self.driver, rows)

        for flag in CUT_HISTORY_FLAGS:
            flags.pop(flag)
        self.change_cluster_version()
        assert scenario.entries().isdisjoint(empty)
        scenario.assert_rows(self.driver, rows)
        rows.update(scenario.write_rows(self.driver, offset=len(rows)))
        scenario.assert_rows(self.driver, rows)
