import os
import tempfile
import unittest

from textual.widgets import ListView

from ydb.tools.mnc.viewer.main import Viewer
from ydb.tools.mnc.viewer.widgets import ConfigCandidate, OverviewStatusCard, _validate_multinode_config


class ClusterConfigValidationTest(unittest.TestCase):
    def test_accepts_config_matching_multinode_scheme(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\nerasure: none\n")

            validation = _validate_multinode_config(path)

        self.assertTrue(validation.ok)
        self.assertEqual(validation.errors, [])

    def test_reports_config_scheme_errors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")

            validation = _validate_multinode_config(path)

        self.assertFalse(validation.ok)
        self.assertTrue(any("erasure" in error for error in validation.errors))


class ClusterConfigSelectionStateTest(unittest.IsolatedAsyncioTestCase):
    async def test_selecting_cluster_config_updates_general_status(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\nerasure: none\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: [ConfigCandidate("cluster", path)]

            async with app.run_test() as pilot:
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "NOT SELECTED",
                )

                await app.run_action("open_cluster_config")
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await pilot.pause()

                self.assertEqual(app._state.cluster_config_status(), "cluster")
                self.assertEqual(app._state.cluster_config_status_kind(), "OK")
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "cluster",
                )

    async def test_failing_cluster_config_is_not_selected(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: [ConfigCandidate("cluster", path)]

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await pilot.pause()

                self.assertIsNone(app._state.selected_cluster_config)
                self.assertEqual(app._state.cluster_config_status(), "NOT SELECTED")
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "NOT SELECTED",
                )
