import os
import tempfile
import unittest
from unittest import mock

import rich.console
from textual.command import CommandPalette
from textual.css.query import NoMatches
from textual.widgets import Button, Checkbox, Input, ListView, TabbedContent

from ydb.tools.mnc.lib import agent_client, deploy_ctx, progress
from ydb.tools.mnc.viewer import main as viewer_main
from ydb.tools.mnc.viewer.main import Viewer
from ydb.tools.mnc.viewer.widgets import (
    AgentHostStatus,
    AgentsState,
    ClusterConfigPane,
    ConfigValidation,
    ConfigCandidate,
    HostCard,
    HostTasksTable,
    MncConfigForm,
    OperationsPane,
    SelectedClusterConfig,
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


class ViewerStartupStateTest(unittest.IsolatedAsyncioTestCase):
    def test_load_mnc_config_uses_defaults_when_yaml_is_malformed(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "mnc.yaml")
            with open(path, "w") as file:
                file.write("git_ydb_root: [")

            with mock.patch.object(viewer_main, "MNC_CONFIG_PATH", path):
                app = Viewer()

        self.assertEqual(app._mnc_config["git_ydb_root"], viewer_main.DEFAULT_GIT_YDB_ROOT)
        self.assertEqual(app._mnc_config["deploy_flags"], [])

    def test_load_mnc_config_uses_defaults_when_yaml_is_not_mapping(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "mnc.yaml")
            with open(path, "w") as file:
                file.write("- not\n- mapping\n")

            with mock.patch.object(viewer_main, "MNC_CONFIG_PATH", path):
                app = Viewer()

        self.assertEqual(app._mnc_config["git_ydb_root"], viewer_main.DEFAULT_GIT_YDB_ROOT)
        self.assertEqual(app._mnc_config["deploy_flags"], [])

    def test_load_mnc_config_uses_defaults_when_file_is_unreadable(self):
        with mock.patch("builtins.open", side_effect=OSError("denied")):
            app = Viewer()

        self.assertEqual(app._mnc_config["git_ydb_root"], viewer_main.DEFAULT_GIT_YDB_ROOT)
        self.assertEqual(app._mnc_config["deploy_flags"], [])

    async def test_check_agents_empty_hosts_is_ok(self):
        app = Viewer()
        refreshed = []
        app._refresh_overview = lambda: refreshed.append(True)

        await app._check_agents([], app._agent_check_generation)

        self.assertEqual(app._state.agents.status, "OK")
        self.assertEqual(app._state.agents.hosts, [])
        self.assertEqual(app._state.agents_details(), "No hosts in selected cluster config")
        self.assertEqual(refreshed, [True])


class ClusterConfigSelectionStateTest(unittest.IsolatedAsyncioTestCase):
    async def test_general_keeps_single_highlighted_card(self):
        app = Viewer()

        async with app.run_test() as pilot:
            await pilot.pause()
            status_row = app.query_one("#overview-status-row", ListView)
            agents_list = app.query_one("#overview-agents-list", ListView)
            self.assertEqual(status_row.index, 0)
            self.assertIsNone(agents_list.index)

            await pilot.press("down")
            self.assertIsNone(status_row.index)
            self.assertEqual(agents_list.index, 0)
            self.assertIs(app.screen.focused, agents_list)

            await pilot.press("up")
            self.assertEqual(status_row.index, 0)
            self.assertIsNone(agents_list.index)

    async def test_selecting_agents_overview_card_opens_agents_tab(self):
        app = Viewer()

        async with app.run_test() as pilot:
            await pilot.click("#overview-agents-title", offset=(1, 0))
            await self._wait_for_active_tab(app, pilot, "agents")

            tabs = app.query_one("#tabs", TabbedContent)
            self.assertEqual(tabs.active, "agents")
            self.assertIn("agents", app._opened_tab_order)
            for _ in range(3):
                await pilot.pause()
                self.assertEqual(tabs.active, "agents")

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
                self.assertEqual(app._state.agents_status(), "OK")
                self.assertEqual(app._state.agents_details(), "Agents: 1/1")

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

                self.assertEqual(app._state.agents_status(), "FAIL")
                self.assertIn("Agents: 1/2", app._state.agents_details())
                self.assertIn("host2: FAIL (boom)", app._state.agents_details())

    async def test_host_details_include_agent_tasks_and_disks(self):
        async def get_json(host, path, port=8999):
            self.assertEqual(host, "host1")
            self.assertEqual(port, 8999)
            if path == "/health":
                return {
                    "status": "healthy",
                    "service": "MNCAgentServer",
                    "enabled_features": ["health", "nodes", "disks", "tasks"],
                }
            if path == "/tasks":
                return {
                    "tasks": [
                        {
                            "id": "old-task",
                            "type": "nodes/start",
                            "status": "completed",
                            "created_at": 1,
                        },
                        {
                            "id": "new-task",
                            "type": "disks/check",
                            "status": "failed",
                            "created_at": 2,
                            "error": "boom",
                        },
                    ],
                }
            raise AssertionError(f"unexpected GET path: {path}")

        async def post_json(host, path, payload, port=8999, parent_task=None):
            self.assertEqual(host, "host1")
            self.assertEqual(path, "/disks/info")
            self.assertEqual(payload, {})
            self.assertEqual(port, 8999)
            self.assertIsNone(parent_task)
            return {
                "disks": [
                    {
                        "partlabel": "ssd1",
                        "path": "/dev/sdb",
                        "size": "100GB",
                        "parts": [{"id": "1"}],
                    },
                ],
            }

        app = Viewer()
        with mock.patch.object(agent_client, "get_json", get_json), \
                mock.patch.object(agent_client, "post_json", post_json):
            host = await app._check_agent_host("host1")

        self.assertEqual(host.status, "Running")
        self.assertEqual(host.enabled_features, ["health", "nodes", "disks", "tasks"])
        self.assertEqual(host.last_tasks[0]["id"], "new-task")
        self.assertEqual(host.disks[0]["partlabel"], "ssd1")

        app = Viewer()
        app._state.agents = AgentsState(status="OK", hosts=[host])
        async with app.run_test() as pilot:
            await app.run_action("open_agents")
            await pilot.pause()

            card = app.query_one(HostCard)
            tasks_table = app.query_one(HostTasksTable)

            self.assertEqual(card.host.host, "host1")
            self.assertEqual(card.host.status, "Running")
            self.assertEqual(tasks_table.row_count, 2)
            self.assertEqual(tasks_table.tasks[0]["id"], "new-task")

    async def test_operations_tab_collects_install_arguments(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )
        app._start_agents_check = lambda: None
        calls = []

        async def execute_operation(request):
            calls.append(request)
            return True

        app._execute_operation = execute_operation

        async with app.run_test() as pilot:
            await app.run_action("open_operation('install')")
            await pilot.pause()

            pane = app.query_one(OperationsPane)
            pane.query_one("#operations-waiting", Input).value = "21"
            pane.query_one("#operations-bin-path", Input).value = "/tmp/ydb"
            pane.query_one("#operations-do-not-init", Checkbox).value = True
            pane.query_one("#operations-ignore-failed-stop", Checkbox).value = True
            pane.query_one("#operations-run", Button).press()

            await self._wait_for_operation_calls(pilot, calls)

        request = calls[0]
        self.assertEqual(request.operation_id, "install")
        self.assertEqual(request.waiting, 21)
        self.assertEqual(request.bin_path, "/tmp/ydb")
        self.assertTrue(request.do_not_init)
        self.assertTrue(request.ignore_failed_stop)

    async def test_operations_install_arguments_arrow_navigation(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )

        async with app.run_test() as pilot:
            await app.run_action("open_operation('install')")
            await pilot.pause()

            pane = app.query_one(OperationsPane)
            pane._focus_operation_form_item(0)
            self.assertIs(app.screen.focused, app.query_one("#operations-waiting", Input))

            pane.move_operation_form_focus_from("operations-waiting", 1)
            self.assertIs(app.screen.focused, app.query_one("#operations-bin-path", Input))

            pane.move_operation_form_focus_from("operations-bin-path", 1)
            self.assertIs(app.screen.focused, app.query_one("#operations-do-not-init", Checkbox))

            pane.move_operation_form_focus_from("operations-do-not-init", 1)
            self.assertIs(app.screen.focused, app.query_one("#operations-ignore-failed-stop", Checkbox))

            pane.move_operation_form_focus_from("operations-ignore-failed-stop", 1)
            self.assertIs(app.screen.focused, app.query_one("#operations-run", Button))

            pane.move_operation_form_focus_from("operations-run", -1)
            self.assertIs(app.screen.focused, app.query_one("#operations-ignore-failed-stop", Checkbox))

    async def test_operations_uninstall_arguments_arrow_navigation(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )

        async with app.run_test() as pilot:
            await app.run_action("open_operation('uninstall')")
            await pilot.pause()

            pane = app.query_one(OperationsPane)
            pane._focus_operation_form_item(0)
            self.assertIs(app.screen.focused, app.query_one("#operations-ignore-failed-stop", Checkbox))

            pane.move_operation_form_focus_from("operations-ignore-failed-stop", 1)
            self.assertIs(app.screen.focused, app.query_one("#operations-run", Button))

            pane.move_operation_form_focus_from("operations-run", -1)
            self.assertIs(app.screen.focused, app.query_one("#operations-ignore-failed-stop", Checkbox))

    async def test_operations_action_opens_operation_picker(self):
        app = Viewer()

        async with app.run_test() as pilot:
            await app.run_action("open_operations")
            await pilot.pause()

            self.assertTrue(CommandPalette.is_open(app))

    async def test_operations_tab_shows_running_steps_and_final_output(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )
        app._start_agents_check = lambda: None
        running_rows = []

        async def act(
            hosts,
            config,
            waiting=None,
            bin_path=None,
            do_not_init=None,
            ignore_failed_stop=None,
            console=None,
        ):
            with progress.MyProgress(console=console) as pbar:
                root = await pbar.get_hidden_task().add_subtask("[bold blue]Deploy[/]", total=2)
                child = await root.add_subtask("Copy binary", total=1)
                await child.update(advance=1)
                backend = app._state.operation.progress_backend
                running_rows.append([(level, state.title) for level, state in backend.visible_rows()])
            console.print("final output")
            return True

        with mock.patch("ydb.tools.mnc.viewer.main.install_command.act", act):
            async with app.run_test() as pilot:
                await app.run_action("open_operation('install')")
                await pilot.pause()

                pane = app.query_one(OperationsPane)
                pane.query_one("#operations-run", Button).press()
                await pilot.pause()

                self.assertFalse(pane.query_one("#operations-form").display)
                self.assertTrue(pane.query_one("#operations-result").display)
                await self._wait_for_operation_status(pilot, app, "OK")

                self.assertFalse(pane.query_one("#operations-live").display)
                self.assertEqual(len(app._state.operation.progress_backend.visible_rows()), 2)
                output = app._state.operation.message

        self.assertTrue(
            any(
                any(level == 0 and "Deploy" in title for level, title in rows)
                and any(level == 1 and "Copy binary" in title for level, title in rows)
                for rows in running_rows
            )
        )
        self.assertIn("final output", output)

    async def test_operations_tab_renders_task_result_panel_after_completion(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )

        async def act(
            hosts,
            config,
            waiting=None,
            bin_path=None,
            do_not_init=None,
            ignore_failed_stop=None,
            console=None,
        ):
            with progress.MyProgress(console=console) as pbar:
                task = await pbar.get_hidden_task().add_subtask("Install", total=1)
                await task.update(advance=1)
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                step_title="Install",
                message="failed step",
            )

        with mock.patch("ydb.tools.mnc.viewer.main.install_command.act", act):
            async with app.run_test() as pilot:
                await app.run_action("open_operation('install')")
                await pilot.pause()

                pane = app.query_one(OperationsPane)
                pane.query_one("#operations-run", Button).press()
                await self._wait_for_operation_status(pilot, app, "FAIL")

                self.assertFalse(pane.query_one("#operations-live").display)
                self.assertIsNotNone(app._state.operation.result_renderable)
                console = rich.console.Console(record=True)
                console.print(pane._operation_output())

        output = console.export_text()
        self.assertIn("ERROR:", output)
        self.assertIn("Install", output)
        self.assertIn("failed step", output)

    def test_operation_result_renderable_shows_successful_steps(self):
        app = Viewer()
        result = progress.TaskResult(
            level=progress.TaskResultLevel.OK,
            step_title="Uninstall",
            subresults=[
                progress.TaskResult(level=progress.TaskResultLevel.OK, step_title="Check agents"),
                progress.TaskResult(level=progress.TaskResultLevel.OK, step_title="Stop hosts"),
                progress.TaskResult(level=progress.TaskResultLevel.OK, step_title="Uninstall multinode"),
            ],
        )

        renderable = app._operation_result_renderable(result)
        console = rich.console.Console(record=True)
        console.print(renderable)
        output = console.export_text()

        self.assertIn("DONE:", output)
        self.assertIn("Check agents", output)
        self.assertIn("Stop hosts", output)
        self.assertIn("Uninstall multinode", output)

    async def test_settings_save_deploy_flags_from_checkboxes(self):
        app = Viewer()
        app._mnc_config = {"git_ydb_root": "/repo", "deploy_flags": []}
        app._save_mnc_config = lambda: None

        async with app.run_test() as pilot:
            await app.run_action("open_mnc_config")
            await pilot.pause()

            self.assertTrue(app.query_one("#mnc-deploy-flag-strip-binary", Checkbox).value)
            with self.assertRaises(NoMatches):
                app.query_one("#mnc-deploy-flag-do_not_strip", Checkbox)

            app.query_one("#mnc-deploy-flag-strip-binary", Checkbox).value = False
            await pilot.pause()
            self.assertIn("do_not_strip", app._mnc_config["deploy_flags"])
            self.assertNotIn("do_strip", app._mnc_config["deploy_flags"])

            app.query_one("#mnc-deploy-flag-strip-binary", Checkbox).value = True
            await pilot.pause()
            self.assertIn("do_strip", app._mnc_config["deploy_flags"])
            self.assertNotIn("do_not_strip", app._mnc_config["deploy_flags"])

            app.query_one("#mnc-deploy-flag-secure-mode", Checkbox).value = True
            await pilot.pause()
            self.assertIn("secure", app._mnc_config["deploy_flags"])

    async def test_settings_arrow_navigation_moves_between_fields_and_deploy_flags(self):
        app = Viewer()

        async with app.run_test() as pilot:
            await app.run_action("open_mnc_config")
            await pilot.pause()

            form = app.query_one(MncConfigForm)
            fields = app.query_one("#mnc-config-fields", ListView)
            form.focus_config_fields()
            self.assertIs(app.screen.focused, fields)
            self.assertEqual(fields.index, 0)

            form.focus_deploy_flag(0, 0)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-rebuild-binary", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-rebuild-binary", col_delta=1)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-strip-binary", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-strip-binary", row_delta=1)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-transit-binary", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-transit-binary", row_delta=1)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-secure-mode", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-secure-mode", row_delta=-1)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-redeploy-binary", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-redeploy-binary", row_delta=-1)
            self.assertIs(app.screen.focused, app.query_one("#mnc-deploy-flag-rebuild-binary", Checkbox))

            form.move_deploy_flag_from("mnc-deploy-flag-rebuild-binary", row_delta=-1)
            self.assertIs(app.screen.focused, fields)

    async def test_operations_prepare_deploy_context_before_install(self):
        app = Viewer()
        app._mnc_config = {"git_ydb_root": "/repo", "deploy_flags": ["do_not_strip", "secure"]}
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none", "deploy_flags": None}),
        )
        app._start_agents_check = lambda: None
        contexts = []

        async def act(
            hosts,
            config,
            waiting=None,
            bin_path=None,
            do_not_init=None,
            ignore_failed_stop=None,
            console=None,
        ):
            contexts.append(
                {
                    "path_to_bin": deploy_ctx.path_to_bin,
                    "work_directory_exists": os.path.isdir(deploy_ctx.work_directory),
                    "do_rebuild": deploy_ctx.do_rebuild,
                    "do_strip": deploy_ctx.do_strip,
                    "secure": deploy_ctx.secure,
                    "is_manual_path_to_bin": deploy_ctx.is_manual_path_to_bin,
                }
            )
            return True

        with mock.patch("ydb.tools.mnc.viewer.main.install_command.act", act):
            async with app.run_test() as pilot:
                await app.run_action("open_operation('install')")
                await pilot.pause()

                pane = app.query_one(OperationsPane)
                pane.query_one("#operations-run", Button).press()
                await self._wait_for_operation_status(pilot, app, "OK")

        self.assertEqual(contexts[0]["path_to_bin"], "/repo/ydb/apps/ydbd/ydbd")
        self.assertTrue(contexts[0]["work_directory_exists"])
        self.assertTrue(contexts[0]["do_rebuild"])
        self.assertFalse(contexts[0]["do_strip"])
        self.assertTrue(contexts[0]["secure"])
        self.assertFalse(contexts[0]["is_manual_path_to_bin"])

    async def test_operations_tab_shows_exception_traceback(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )

        async def execute_operation(request):
            raise AttributeError("'NoneType' object has no attribute 'endswith'")

        app._execute_operation = execute_operation

        async with app.run_test() as pilot:
            await app.run_action("open_operation('install')")
            await pilot.pause()

            pane = app.query_one(OperationsPane)
            pane.query_one("#operations-run", Button).press()
            await self._wait_for_operation_status(pilot, app, "FAIL")

        output = app._state.operation.message
        self.assertIn("Backtrace:", output)
        self.assertIn("AttributeError", output)
        self.assertIn("'NoneType' object has no attribute 'endswith'", output)
        self.assertEqual(app._state.operation.error_type, "AttributeError")
        self.assertEqual(app._state.operation.error_message, "'NoneType' object has no attribute 'endswith'")
        self.assertTrue(app._state.operation.backtrace)

    async def test_operations_tab_disables_run_for_invalid_waiting(self):
        app = Viewer()
        app._state.selected_cluster_config = SelectedClusterConfig(
            ConfigCandidate("cluster", "/tmp/cluster.yaml"),
            ConfigValidation([], {"hosts": ["host1"], "erasure": "none"}),
        )

        async with app.run_test() as pilot:
            await app.run_action("open_operation('install')")
            await pilot.pause()

            pane = app.query_one(OperationsPane)
            pane.query_one("#operations-waiting", Input).value = "0"
            await pilot.pause()

            self.assertTrue(pane.query_one("#operations-run", Button).disabled)

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

    async def _wait_for_active_tab(self, app, pilot, tab_id):
        for _ in range(10):
            if app.query_one("#tabs", TabbedContent).active == tab_id:
                return
            await pilot.pause()
        self.fail(f"active tab did not become {tab_id}")

    async def _wait_for_operation_calls(self, pilot, calls):
        for _ in range(10):
            if calls:
                return
            await pilot.pause()
        self.fail("operation was not executed")

    async def _wait_for_operation_status(self, pilot, app, status):
        for _ in range(10):
            if app._state.operation.status == status:
                return
            await pilot.pause()
        self.fail(f"operation status did not become {status}")
