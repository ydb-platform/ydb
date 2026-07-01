import argparse
import os
import tempfile
import types
import unittest
from unittest import mock

import rich.console

from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.lib.exceptions import CliError

from ydb.tools.mnc.cli import arg_metadata, command_options, parser_factory
from ydb.tools.mnc.cli.tui import app as tui_app
from ydb.tools.mnc.cli.tui.app import TuiApp
from ydb.tools.mnc.cli.tui.command_picker import CommandPickerApp, _BackListItem
from ydb.tools.mnc.cli.tui.common import ConfigCandidate, config_preview
from ydb.tools.mnc.cli.tui.launcher import LauncherResult, TuiLauncher, should_route_to_launcher
from ydb.tools.mnc.lib import output
from ydb.tools.mnc.lib.output import VerbosityMode
from ydb.tools.mnc.lib.progress_live import LiveBackend


def _reset_output():
    output._state = {
        "mode": VerbosityMode.NORMAL,
        "console": None,
        "stderr_console": None,
        "active_progress": None,
        "progress_backend_override": None,
    }
    output.init(VerbosityMode.NORMAL)


class LiveBackendTest(unittest.TestCase):
    def test_tree_selection_and_logs_render(self):
        backend = LiveBackend(console=rich.console.Console(record=True))
        root = backend.add_task("root", total=2)
        child = backend.add_task("child", total=1, parent=root)
        backend.update(child, completed=1)
        backend.append_log("hello", step_id="12345678-1234-1234-1234-123456789abc")

        rows = backend.visible_rows()
        self.assertEqual([state.title for _, state in rows], ["root", "child"])
        backend.move_selection(1)
        self.assertEqual(backend.selected_task().title, "child")
        self.assertIsNotNone(backend.render())
        self.assertIn("hello", str(backend.render_logs().renderable))

    def test_toggle_selected_collapses_children(self):
        backend = LiveBackend(console=rich.console.Console(record=True))
        root = backend.add_task("root")
        backend.add_task("child", parent=root)
        self.assertEqual(len(backend.visible_rows()), 2)
        backend.toggle_selected()
        self.assertEqual(len(backend.visible_rows()), 1)

    def test_log_window_width_matches_right_pane(self):
        backend = LiveBackend(console=rich.console.Console(record=True, width=120))
        self.assertEqual(backend.log_window_width(), 76)

    def test_log_payload_width_accounts_for_step_prefix(self):
        backend = LiveBackend(console=rich.console.Console(record=True, width=120))

        self.assertEqual(backend.log_prefix_width("12345678-1234-1234-1234-123456789abc"), 9)
        self.assertEqual(backend.log_payload_width("12345678-1234-1234-1234-123456789abc"), 67)

    def test_textual_body_renderables_are_available_without_panels(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        root = backend.add_task("root", total=2)
        backend.update(root, completed=1)
        backend.append_log("hello", step_id="12345678-1234-1234-1234-123456789abc")

        console.print(backend.render_tree_body())
        self.assertIn("root", console.export_text())
        self.assertIn("Task id", backend.render_details_body().plain)
        self.assertIn("hello", backend.render_logs_body().plain)

    def test_markup_in_task_title_uses_rich_markup_for_textual_body_renderables(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        backend.add_task("[bold]Install[/]", total=1)

        console.print(backend.render_tree_body())
        details = backend.render_details_body()

        self.assertIn("Install", console.export_text())
        self.assertIn("Install", details.plain)
        self.assertNotIn("[bold]", details.plain)

    def test_invalid_markup_in_task_title_is_rendered_as_plain_text(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        backend.add_task("Install[/]", total=1)

        console.print(backend.render_tree_body())
        details = backend.render_details_body()

        self.assertIn("Install[/]", console.export_text())
        self.assertIn("Install[/]", details.plain)


class TuiLauncherRoutingTest(unittest.TestCase):
    def test_no_verb_routes_to_launcher(self):
        args = argparse.Namespace(verb=None, tui=False)
        self.assertTrue(should_route_to_launcher(args, {}, {}))

    def test_install_without_tui_does_not_route_by_metadata(self):
        args = argparse.Namespace(verb="install", tui=False)
        self.assertFalse(should_route_to_launcher(args, {"install": object()}, {"install": True}))

    def test_complete_non_tui_command_does_not_route(self):
        args = argparse.Namespace(verb="service", tui=False, config_name="cfg", config_path=None)
        self.assertFalse(should_route_to_launcher(args, {"service": object()}, {"service": False}))

    def test_tui_missing_config_routes_to_launcher(self):
        args = argparse.Namespace(verb="service", tui=True, config_name=None, config_path=None)
        self.assertTrue(should_route_to_launcher(args, {"service": object()}, {"service": False}))

    def test_metadata_extracts_commands_and_options(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)

        install = arg_metadata.find_command(root, ["install"])

        self.assertIsNotNone(install)
        self.assertTrue(install.is_leaf)
        self.assertIn("waiting", {option.dest for option in install.options})
        self.assertIn("do_not_init", {option.dest for option in install.options})

    def test_common_options_are_detected_by_group(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        launcher = TuiLauncher(parser, expected_config)

        common_dests = {option.dest for option in launcher._common_options(install)}
        command_dests = {option.dest for option in launcher._command_options(install)}

        self.assertIn("work_directory", common_dests)
        self.assertIn("waiting", command_dests)
        self.assertIn("do_not_init", command_dests)
        self.assertNotIn("waiting", common_dests)

    def test_command_picker_can_be_constructed(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)

        app = CommandPickerApp(root)

        self.assertIs(app.command_parent, root)
        self.assertEqual(app.stack, [root])
        self.assertTrue(root.children)

    def test_textual_apps_use_consistent_light_theme_friendly_panel_css(self):
        from ydb.tools.mnc.cli.tui.app import RuntimeProgressApp
        from ydb.tools.mnc.cli.tui.config_picker import ConfigPickerApp
        from ydb.tools.mnc.cli.tui.options_form import OptionsFormApp

        for app_cls in (CommandPickerApp, ConfigPickerApp, OptionsFormApp, RuntimeProgressApp):
            self.assertIn("border: solid $primary", app_cls.CSS)
            self.assertIn("background: transparent", app_cls.CSS)
            self.assertNotIn("background: $surface", app_cls.CSS)

    def test_command_picker_can_start_from_subcommand_level(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])

        app = CommandPickerApp(root, initial=service)

        self.assertIs(app.command_parent, service)
        self.assertEqual(app.stack, [root, service])
        self.assertTrue(service.children)

    def test_command_picker_back_does_not_cancel_at_nested_level(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])
        app = CommandPickerApp(root, initial=service)

        self.assertTrue(app._pop_command_level())

        self.assertIs(app.command_parent, root)
        self.assertEqual(app.stack, [root])
        self.assertFalse(app._pop_command_level())

    def test_command_picker_ignores_stale_back_highlight_at_root(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        app = CommandPickerApp(root)
        event = types.SimpleNamespace(
            item=_BackListItem(),
            stop=mock.Mock(),
        )

        app.on_list_view_highlighted(event)

        event.stop.assert_called_once()

    def test_command_help_text_renders_parser_help(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])

        help_text = arg_metadata.command_help_text(service)

        self.assertIn("Usage:", help_text)
        self.assertIn("Subcommands:", help_text)

    def test_config_preview_reads_config_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate)

        self.assertIn("cluster", preview.plain)
        self.assertIn(path, preview.plain)
        self.assertIn("host1", preview.plain)

    def test_config_preview_reports_validation_errors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate, {"__type__": dict, "missing": str})

        self.assertIn("Config is not compatible", preview.plain)
        self.assertIn("missing", preview.plain)

    def test_config_preview_has_no_validation_error_for_matching_config(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate, {"__type__": dict, "hosts": {"__type__": list, "__inner__": str}})

        self.assertNotIn("Config is not compatible", preview.plain)

    def test_launcher_values_to_argv_skips_defaults_and_keeps_changes(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        args = parser.parse_args(["install"])
        launcher = TuiLauncher(parser, expected_config)
        values = {
            "flags": {"do_not_init": True, "ignore_failed_stop": False},
            "options": {"waiting": "20", "bin_path": ""},
            "arguments": {},
        }

        argv = launcher._values_to_argv(install, values, args)

        self.assertIn("--do-not-init", argv)
        self.assertIn("--waiting", argv)
        self.assertIn("20", argv)
        self.assertNotIn("--ignore-failed-stop", argv)
        self.assertNotIn("--bin-path", argv)

    def test_launcher_values_to_argv_accepts_deploy_flags_list(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        args = parser.parse_args(["install"])
        launcher = TuiLauncher(parser, expected_config)
        values = {
            "flags": {},
            "options": {"deploy_flags": ["do_rebuild", "secure"]},
            "arguments": {},
        }

        argv = launcher._values_to_argv(install, values, args)

        self.assertEqual(argv, ["--deploy-flags", "do_rebuild", "secure"])

    def test_launcher_prefills_selected_command_from_cached_options(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        qemu_run = arg_metadata.find_command(root, ["qemu", "run"])
        launcher = TuiLauncher(parser, expected_config)
        initial_args = parser.parse_args([])

        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HOME": home}):
                command_options.save_cache({
                    "qemu/run": {
                        "tokens": [
                            "--config",
                            "cfg1",
                            "--host",
                            "host1",
                            "--disk-id",
                            "disk1",
                            "--ssh-port",
                            "2222",
                        ],
                    },
                })

                args = launcher._initial_args_with_cached_options(qemu_run, initial_args)

        self.assertEqual(args.verb, "qemu")
        self.assertEqual(args.cmd, "run")
        self.assertEqual(args.config_name, "cfg1")
        self.assertEqual(args.host, "host1")
        self.assertEqual(args.disk_id, "disk1")
        self.assertEqual(args.ssh_port, 2222)

    def test_launcher_prefill_keeps_explicit_initial_argv_values(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        qemu_run = arg_metadata.find_command(root, ["qemu", "run"])
        launcher = TuiLauncher(parser, expected_config)
        initial_args = parser.parse_args([])

        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HOME": home}):
                command_options.save_cache({
                    "qemu/run": {
                        "tokens": [
                            "--config",
                            "cfg1",
                            "--host",
                            "host1",
                            "--disk-id",
                            "old-disk",
                        ],
                    },
                })

                args = launcher._initial_args_with_cached_options(
                    qemu_run,
                    initial_args,
                    ["qemu", "run", "--disk-id", "new-disk"],
                )

        self.assertEqual(args.config_name, "cfg1")
        self.assertEqual(args.host, "host1")
        self.assertEqual(args.disk_id, "new-disk")

    def test_launcher_keeps_initial_args_when_cache_is_invalid(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        qemu_run = arg_metadata.find_command(root, ["qemu", "run"])
        launcher = TuiLauncher(parser, expected_config)
        initial_args = parser.parse_args([])

        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HOME": home}):
                command_options.save_cache({
                    "qemu/run": {"tokens": ["--unknown-option"]},
                })

                args = launcher._initial_args_with_cached_options(qemu_run, initial_args)

        self.assertIs(args, initial_args)


class TuiAppErrorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output()

    @staticmethod
    async def _run_runtime_app(runtime_app):
        try:
            result = await runtime_app.action(runtime_app.progress_context)
            runtime_app.backend.result = result
        except Exception as exc:
            runtime_app.error = exc
            runtime_app.backend.result = runtime_app.result_from_exception(exc)
        return runtime_app.backend.result

    async def test_run_async_prints_rich_error_after_live_exit_and_marks_exception(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(pbar):
            raise CliError("boom")

        with mock.patch.object(tui_app.RuntimeProgressApp, "run_async", new=self._run_runtime_app):
            with self.assertRaises(CliError) as error:
                await app.run_async(action)

        self.assertTrue(getattr(error.exception, "_mnc_tui_reported", False))
        rendered = console.export_text()
        self.assertIn("ERROR: Command", rendered)
        self.assertIn("boom", rendered)

    async def test_run_async_prints_cli_error_task_result(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)
        command_result = progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            step_title="failed-step",
            message="actionable failure",
        )

        async def action(pbar):
            raise CliError("generic failure", result=command_result)

        with mock.patch.object(tui_app.RuntimeProgressApp, "run_async", new=self._run_runtime_app):
            with self.assertRaises(CliError) as error:
                await app.run_async(action)

        self.assertTrue(getattr(error.exception, "_mnc_tui_reported", False))
        rendered = console.export_text()
        self.assertIn("ERROR: failed-step", rendered)
        self.assertIn("actionable failure", rendered)
        self.assertNotIn("generic failure", rendered)

    async def test_run_async_prints_compact_task_result(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(parent_task=None):
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                step_title="root",
                subresults=[
                    progress.TaskResult(
                        level=progress.TaskResultLevel.OK,
                        step_title="successful_subtree",
                        subresults=[
                            progress.TaskResult(
                                level=progress.TaskResultLevel.ERROR,
                                step_title="recovered_error",
                                message="recovered",
                            ),
                        ],
                    ),
                    progress.TaskResult(
                        level=progress.TaskResultLevel.ERROR,
                        step_title="failed_child",
                        message="failed",
                    ),
                ],
            )

        with mock.patch.object(tui_app.RuntimeProgressApp, "run_async", new=self._run_runtime_app):
            await app.run_async(action)
        rendered = console.export_text()

        self.assertIn("failed_child", rendered)
        self.assertNotIn("successful_subtree", rendered)
        self.assertNotIn("recovered_error", rendered)

    async def test_run_async_prints_task_result_error(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(pbar):
            return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message="failed step")

        with mock.patch.object(tui_app.RuntimeProgressApp, "run_async", new=self._run_runtime_app):
            result = await app.run_async(action)

        self.assertFalse(result)
        self.assertIn("failed step", console.export_text())


class TuiMainRoutingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output()

    async def _run_tui_action(self, action):
        return await action(None)

    async def test_async_main_no_args_runs_launcher(self):
        from ydb.tools.mnc.cli import main

        async def do(args):
            return True

        module = types.SimpleNamespace(
            __name__="install",
            expected_config=None,
            prefer_tui_launcher=True,
            add_arguments=lambda parser: None,
            do=do,
        )
        parser, actions, expected_config, prefer_launcher = parser_factory.build_parser([module])
        launcher_result = LauncherResult(args=parser.parse_args(["install"]), argv=["install"])

        with mock.patch("sys.argv", ["mnc"]), \
             mock.patch.object(main.parser_factory, "build_parser", return_value=(parser, actions, expected_config, prefer_launcher)), \
             mock.patch.object(main.TuiLauncher, "run_async", return_value=launcher_result), \
             mock.patch.object(main.TuiApp, "run_async", side_effect=self._run_tui_action):
            await main.async_main()

    async def test_async_main_install_does_not_run_launcher_without_tui_flag(self):
        from ydb.tools.mnc.cli import main

        async def do(args):
            return True

        module = types.SimpleNamespace(
            __name__="install",
            expected_config=None,
            prefer_tui_launcher=True,
            add_arguments=lambda parser: None,
            do=do,
        )
        parser, actions, expected_config, prefer_launcher = parser_factory.build_parser([module])

        with mock.patch("sys.argv", ["mnc", "install"]), \
             mock.patch.object(main.parser_factory, "build_parser", return_value=(parser, actions, expected_config, prefer_launcher)), \
             mock.patch.object(main.TuiLauncher, "run_async") as run_launcher, \
             mock.patch.object(main.TuiApp, "run_async", side_effect=self._run_tui_action) as run_tui:
            await main.async_main()

        run_launcher.assert_not_called()
        run_tui.assert_not_called()


if __name__ == '__main__':
    unittest.main()
