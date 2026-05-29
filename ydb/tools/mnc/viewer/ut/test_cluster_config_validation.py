import os
import tempfile
import unittest

from textual.widgets import ListView

from ydb.tools.mnc.viewer.main import Viewer
from ydb.tools.mnc.viewer.widgets import (
    AgentHostStatus,
    ClusterConfigPane,
    ConfigCandidate,
    OverviewStatusCard,
    _validate_multinode_config,
)


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
            app._check_agent_host = self._check_agent_ok

            async with app.run_test() as pilot:
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "NOT SELECTED",
                )

                await app.run_action("open_cluster_config")
                await pilot.pause()
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await pilot.pause()

                self.assertEqual(app._state.cluster_config_status(), "cluster")
                self.assertEqual(app._state.cluster_config_status_kind(), "OK")
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "cluster",
                )
                await self._wait_for_agents_status(app, pilot, "OK")
                self.assertEqual(app._state.agents_status(), "OK 1/1")

    async def test_failing_cluster_config_is_not_selected(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: [ConfigCandidate("cluster", path)]

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                await pilot.pause()
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await pilot.pause()

                self.assertIsNone(app._state.selected_cluster_config)
                self.assertEqual(app._state.cluster_config_status(), "NOT SELECTED")
                self.assertEqual(
                    app.query_one("#overview-cluster-config-card", OverviewStatusCard).status,
                    "NOT SELECTED",
                )

    async def test_agent_status_shows_host_failures_in_general(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n  - host2\nerasure: none\n")

            async def check_agent(host):
                if host == "host1":
                    return AgentHostStatus(host, "OK")
                return AgentHostStatus(host, "FAIL", "boom")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: [ConfigCandidate("cluster", path)]
            app._check_agent_host = check_agent

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                await pilot.pause()
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await self._wait_for_agents_status(app, pilot, "FAIL")

                self.assertEqual(app._state.agents_status(), "FAIL 1/2")
                self.assertIn("host2: FAIL (boom)", app._state.agents_details())

    async def test_rename_selected_cluster_config_updates_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\nerasure: none\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: self._configs_in(tmp_dir)
            app._check_agent_host = self._check_agent_ok

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                await pilot.pause()
                app.query_one("#cluster-configs", ListView).action_select_cursor()
                await self._wait_for_agents_status(app, pilot, "OK")

                pane = app.query_one(ClusterConfigPane)
                pane._rename_candidate(pane._highlighted_candidate(), "renamed")
                await pilot.pause()

                renamed_path = os.path.join(tmp_dir, "renamed.yaml")
                self.assertFalse(os.path.exists(path))
                self.assertTrue(os.path.exists(renamed_path))
                self.assertEqual(app._state.selected_cluster_config.candidate.name, "renamed")
                self.assertEqual(app._state.selected_cluster_config.candidate.path, renamed_path)

    async def test_copy_cluster_config_creates_new_file_without_selecting_it(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\nerasure: none\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: self._configs_in(tmp_dir)

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                await pilot.pause()

                pane = app.query_one(ClusterConfigPane)
                pane._copy_candidate(pane._highlighted_candidate(), "cluster_copy")
                await pilot.pause()

                copied_path = os.path.join(tmp_dir, "cluster_copy.yaml")
                self.assertTrue(os.path.exists(copied_path))
                self.assertIsNone(app._state.selected_cluster_config)

    async def test_edit_cluster_config_revalidates_and_selects_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")

            def edit_file(path):
                with open(path, "w") as file:
                    file.write("hosts:\n  - host1\nerasure: none\n")

            app = Viewer()
            app._discover_cluster_config_candidates = lambda: self._configs_in(tmp_dir)
            app._check_agent_host = self._check_agent_ok
            app._edit_file = lambda path: edit_file(path)

            async with app.run_test() as pilot:
                await app.run_action("open_cluster_config")
                await pilot.pause()

                app.query_one(ClusterConfigPane).action_edit_config()
                await self._wait_for_agents_status(app, pilot, "OK")

                self.assertEqual(app._state.selected_cluster_config.candidate.path, path)
                self.assertEqual(app._state.cluster_config_status(), "cluster")

    async def _check_agent_ok(self, host):
        return AgentHostStatus(host, "OK")

    def _configs_in(self, directory):
        return [
            ConfigCandidate(os.path.splitext(name)[0], os.path.join(directory, name))
            for name in sorted(os.listdir(directory))
            if name.endswith((".yaml", ".yml"))
        ]

    async def _wait_for_agents_status(self, app, pilot, status):
        for _ in range(10):
            if app._state.agents.status == status:
                return
            await pilot.pause()
        self.fail(f"agents status did not become {status}: {app._state.agents.status}")
