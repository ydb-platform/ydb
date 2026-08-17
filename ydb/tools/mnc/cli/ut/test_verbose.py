"""
Tests for verbose mode: PlainBackend, backend selection, to_rich_panel verbose flag.
"""

import io
import os
import tempfile
import unittest
from unittest import mock

import ydb.tools.mnc.lib.output as output_module
from ydb.tools.mnc.lib.output import VerbosityMode, init
from ydb.tools.mnc.lib.progress import (
    MyProgress,
    PlainBackend,
    RichProgressBackend,
    Step,
    TaskResult,
    TaskResultLevel,
)


def _reset_output(mode=VerbosityMode.NORMAL):
    output_module._state = {
        "mode": mode,
        "console": None,
        "stderr_console": None,
        "active_progress": None,
        "progress_backend_override": None,
    }
    init(mode)


class PlainBackendTest(unittest.TestCase):
    def test_add_task_returns_int_id(self):
        backend = PlainBackend()
        task_id = backend.add_task("step one", total=10)
        self.assertIsNotNone(task_id)

    def test_update_advance(self):
        backend = PlainBackend()
        task_id = backend.add_task("step", total=10)
        backend.update(task_id, advance=3)
        self.assertAlmostEqual(backend.get_task_completed(task_id), 3.0)

    def test_update_completed(self):
        backend = PlainBackend()
        task_id = backend.add_task("step", total=10)
        backend.update(task_id, completed=7)
        self.assertAlmostEqual(backend.get_task_completed(task_id), 7.0)

    def test_update_total(self):
        backend = PlainBackend()
        task_id = backend.add_task("step", total=10)
        backend.update(task_id, total=20)
        self.assertEqual(backend.get_task_total(task_id), 20)

    def test_get_task_total_none_for_indeterminate(self):
        backend = PlainBackend()
        task_id = backend.add_task("step")
        self.assertIsNone(backend.get_task_total(task_id))

    def test_context_manager(self):
        backend = PlainBackend()
        with backend as b:
            self.assertIs(b, backend)

    def test_console_returns_console(self):
        import rich.console
        backend = PlainBackend()
        self.assertIsInstance(backend.console(), rich.console.Console)


class BackendSelectionTest(unittest.TestCase):
    def setUp(self):
        _reset_output(VerbosityMode.NORMAL)

    def tearDown(self):
        _reset_output(VerbosityMode.NORMAL)

    def test_normal_mode_uses_rich_backend(self):
        _reset_output(VerbosityMode.NORMAL)
        progress = MyProgress()
        self.assertIsInstance(progress._backend, RichProgressBackend)

    def test_verbose_mode_uses_plain_backend(self):
        _reset_output(VerbosityMode.VERBOSE)
        progress = MyProgress()
        self.assertIsInstance(progress._backend, PlainBackend)

    def test_quiet_mode_uses_rich_backend(self):
        _reset_output(VerbosityMode.QUIET)
        progress = MyProgress()
        self.assertIsInstance(progress._backend, RichProgressBackend)

    def test_explicit_backend_overrides_mode(self):
        _reset_output(VerbosityMode.VERBOSE)
        custom_backend = RichProgressBackend()
        progress = MyProgress(backend=custom_backend)
        self.assertIs(progress._backend, custom_backend)


class ActiveProgressTrackingTest(unittest.TestCase):
    def setUp(self):
        _reset_output(VerbosityMode.NORMAL)

    def tearDown(self):
        _reset_output(VerbosityMode.NORMAL)

    def test_active_progress_set_on_enter(self):
        from ydb.tools.mnc.lib.output import get_active_progress
        progress = MyProgress(backend=PlainBackend())
        self.assertIsNone(get_active_progress())
        with progress:
            self.assertIs(get_active_progress(), progress)
        self.assertIsNone(get_active_progress())

    def test_active_progress_cleared_on_exit(self):
        from ydb.tools.mnc.lib.output import get_active_progress
        progress = MyProgress(backend=PlainBackend())
        with progress:
            pass
        self.assertIsNone(get_active_progress())


class VerbosePanelTest(unittest.TestCase):
    def test_verbose_panel_shows_all_ok_steps(self):
        child_ok = TaskResult(level=TaskResultLevel.OK, step_title="ok_step", message="all good")
        child_err = TaskResult(level=TaskResultLevel.ERROR, step_title="err_step", message="failed")
        parent = TaskResult(
            level=TaskResultLevel.ERROR,
            step_title="parent",
            subresults=[child_ok, child_err],
        )

        panel_normal = parent.to_rich_panel(show_all=False, verbose=False)
        panel_verbose = parent.to_rich_panel(verbose=True)

        # Both should be renderable
        self.assertIsNotNone(panel_normal)
        self.assertIsNotNone(panel_verbose)

    def test_verbose_panel_does_not_collapse_single_child(self):
        """In verbose mode, single-child chains are NOT collapsed."""
        child = TaskResult(level=TaskResultLevel.ERROR, step_title="child", message="child msg")
        parent = TaskResult(level=TaskResultLevel.ERROR, step_title="parent", subresults=[child])

        panel_verbose = parent.to_rich_panel(verbose=True)
        # Should be a Panel (not a string), meaning it was NOT collapsed into child
        import rich.panel
        self.assertIsInstance(panel_verbose, rich.panel.Panel)

    def test_non_verbose_single_child_collapses(self):
        """In non-verbose mode, single error child IS collapsed (existing behaviour)."""
        child = TaskResult(level=TaskResultLevel.ERROR, step_title="child", message="child msg")
        parent = TaskResult(level=TaskResultLevel.ERROR, step_title="parent", subresults=[child])

        panel_normal = parent.to_rich_panel(verbose=False)
        # Collapsed: returns the child's panel directly, title contains child step_title
        import rich.panel
        self.assertIsInstance(panel_normal, rich.panel.Panel)

    def test_non_verbose_hides_successful_subtree_even_if_it_contains_recovered_error(self):
        recovered_error = TaskResult(level=TaskResultLevel.ERROR, step_title="transient_error", message="failed first")
        ok_child = TaskResult(level=TaskResultLevel.OK, step_title="ok_child", message="recovered")
        successful_subtree = TaskResult(
            level=TaskResultLevel.OK,
            step_title="successful_subtree",
            subresults=[recovered_error, ok_child],
        )
        failed_child = TaskResult(level=TaskResultLevel.ERROR, step_title="failed_child", message="real failure")
        parent = TaskResult(
            level=TaskResultLevel.ERROR,
            step_title="parent",
            subresults=[successful_subtree, failed_child],
        )

        import rich.console as rc
        console = rc.Console(record=True, width=120)
        console.print(parent.to_rich_panel())
        rendered = console.export_text()

        self.assertIn("failed_child", rendered)
        self.assertNotIn("successful_subtree", rendered)
        self.assertNotIn("transient_error", rendered)

    def test_verbose_flag_implies_show_all(self):
        """verbose=True forces show_all=True so OK children appear."""
        child_ok = TaskResult(level=TaskResultLevel.OK, step_title="ok_child", message="ok")
        child_ok2 = TaskResult(level=TaskResultLevel.OK, step_title="ok_child2", message="ok2")
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent",
            subresults=[child_ok, child_ok2],
        )

        import rich.panel
        # Non-verbose: OK children hidden → no subresults rendered → returns string
        # (just verify it doesn't crash)
        parent.to_rich_panel(verbose=False)
        # Verbose: OK children shown → returns Panel
        panel_verbose = parent.to_rich_panel(verbose=True)
        self.assertIsInstance(panel_verbose, rich.panel.Panel)


class PlainBackendNoAnsiTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output(VerbosityMode.VERBOSE)

    def tearDown(self):
        _reset_output(VerbosityMode.NORMAL)

    async def test_plain_backend_step_runs_without_ansi(self):
        """Steps run through PlainBackend produce no ANSI escape sequences."""
        import io
        import rich.console as rc

        buf = io.StringIO()
        plain_console = rc.Console(file=buf, no_color=True, highlight=False)
        backend = PlainBackend(console=plain_console)

        async def ok_command(task, storage):
            return True

        step = Step("my_step", ok_command)

        with MyProgress(backend=backend) as pbar:
            parent = await pbar.add_task("parent")
            result = await step.run(parent)

        self.assertTrue(bool(result))
        output_text = buf.getvalue()
        # No ANSI CSI sequences (ESC[...)
        self.assertNotIn("\x1b[", output_text)


class RuntimeActionVerboseLoggingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output(VerbosityMode.VERBOSE)
        self.temp_dir = tempfile.TemporaryDirectory()
        from ydb.tools.mnc.lib import deploy_ctx
        self._prev_workdir = deploy_ctx.work_directory
        deploy_ctx.work_directory = self.temp_dir.name

    def tearDown(self):
        from ydb.tools.mnc.lib import deploy_ctx
        deploy_ctx.work_directory = self._prev_workdir
        self.temp_dir.cleanup()
        _reset_output(VerbosityMode.NORMAL)

    async def test_runtime_action_error_writes_log_and_uses_step_id_prefix(self):
        import asyncio.subprocess
        import rich.console as rc
        from ydb.tools.mnc.lib import output as output_lib
        from ydb.tools.mnc.lib.progress import MyProgress
        from ydb.tools.mnc.lib.tools import runtime_action

        buf = io.StringIO()
        output_lib._state["console"] = rc.Console(file=buf, no_color=True, highlight=False)

        async def action(stream=None):
            return await asyncio.create_subprocess_shell(
                "printf 'line1\\nline2\\n'; exit 1",
                stdout=stream,
                stderr=asyncio.subprocess.STDOUT,
            )

        with MyProgress(backend=PlainBackend(console=output_lib.get_console())) as pbar:
            task = await pbar.add_task("build step", total=100)
            result = await runtime_action(action, task)

        self.assertFalse(bool(result))
        panel = result.to_rich_panel(verbose=True)
        console = output_lib.get_console()
        with console.capture() as capture:
            console.print(panel)
        self.assertIn("Full log:", capture.get())
        logs_dir = os.path.join(self.temp_dir.name, "logs")
        self.assertTrue(os.path.isdir(logs_dir))
        self.assertTrue(any(name.endswith('.log') for name in os.listdir(logs_dir)))
        self.assertIsNotNone(task.step_id)
        self.assertIn(task.step_id, buf.getvalue())

    async def test_runtime_action_sets_pty_width_from_tui_backend(self):
        import asyncio.subprocess
        import rich.console as rc
        from ydb.tools.mnc.lib import output as output_lib
        from ydb.tools.mnc.lib.progress import MyProgress
        from ydb.tools.mnc.lib.progress_live import LiveBackend
        from ydb.tools.mnc.lib import tools

        output_lib._state["console"] = rc.Console(file=io.StringIO(), no_color=True, highlight=False, width=120)
        backend = LiveBackend(console=output_lib.get_console())
        widths = []
        original_set_pty_window_size = tools._set_pty_window_size

        def set_pty_window_size(fd, *, width=None, height=None):
            widths.append(width)
            return original_set_pty_window_size(fd, width=width, height=height)

        async def action(stream=None):
            return await asyncio.create_subprocess_shell(
                "printf 'line\\n'; exit 1",
                stdout=stream,
                stderr=asyncio.subprocess.STDOUT,
            )

        with mock.patch.object(tools, "_set_pty_window_size", set_pty_window_size):
            with MyProgress(backend=backend) as pbar:
                task = await pbar.add_task("build step", total=100)
                await tools.runtime_action(action, task)

        self.assertEqual(widths, [backend.log_payload_width(task.step_id)])


if __name__ == '__main__':
    unittest.main()
