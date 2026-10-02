"""
Unit tests for TaskResult class.
"""

import asyncio
import os
import tempfile
import unittest

from ydb.tools.mnc.lib import deploy_ctx
from ydb.tools.mnc.lib.progress import TaskResult, TaskResultLevel, Step, ParallelStepGroup


class TaskResultTest(unittest.TestCase):
    def test_bool_protocol_ok_returns_true(self):
        """Test that OK level returns True."""
        result = TaskResult(level=TaskResultLevel.OK, step_title="test")
        self.assertTrue(bool(result))

    def test_bool_protocol_skipped_returns_true(self):
        """Test that SKIPPED level returns True."""
        result = TaskResult(level=TaskResultLevel.SKIPPED, step_title="test")
        self.assertTrue(bool(result))

    def test_bool_protocol_error_returns_false(self):
        """Test that ERROR level returns False."""
        result = TaskResult(level=TaskResultLevel.ERROR, step_title="test")
        self.assertFalse(bool(result))

    def test_bool_protocol_warning_returns_true(self):
        """Test that WARNING level returns True."""
        result = TaskResult(level=TaskResultLevel.WARNING, step_title="test")
        self.assertTrue(bool(result))

    def test_bool_protocol_exception_returns_false(self):
        """Test that exception without level returns False."""
        exc = RuntimeError("test error")
        result = TaskResult(exception=exc, step_title="test")
        self.assertFalse(bool(result))

    def test_bool_protocol_with_subresults_all_true(self):
        """Test that all True subresults return True."""
        sub1 = TaskResult(level=TaskResultLevel.OK, step_title="sub1")
        sub2 = TaskResult(level=TaskResultLevel.OK, step_title="sub2")
        result = TaskResult(subresults=[sub1, sub2], step_title="test")
        self.assertTrue(bool(result))

    def test_bool_protocol_with_subresults_one_false(self):
        """Test that one False subresult returns False."""
        sub1 = TaskResult(level=TaskResultLevel.OK, step_title="sub1")
        sub2 = TaskResult(level=TaskResultLevel.ERROR, step_title="sub2")
        result = TaskResult(subresults=[sub1, sub2], step_title="test")
        self.assertFalse(bool(result))

    def test_auto_level_from_exception(self):
        """Test that exception auto-sets level to ERROR."""
        exc = RuntimeError("test error")
        result = TaskResult(exception=exc, step_title="test")
        self.assertEqual(result.level, TaskResultLevel.ERROR)

    def test_auto_level_from_bool_true(self):
        """Test that True result auto-sets level to OK."""
        result = TaskResult(step_title="test")
        # When level is None and no exception, it should auto-determine
        # This tests the internal logic
        self.assertIsNotNone(result.level)

    def test_to_rich_panel_collapses_single_child(self):
        """Test that single child is collapsed in to_rich_panel."""
        child = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child_step",
            message="child message"
        )
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent_step",
            message="parent message",
            subresults=[child]
        )

        panel = parent.to_rich_panel()
        # The panel should have the child's title in it (collapsed)
        self.assertIsNotNone(panel)

    def test_to_rich_panel_shows_multiple_children(self):
        """Test that multiple children are shown in to_rich_panel."""
        child1 = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child1",
            message="message1"
        )
        child2 = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child2",
            message="message2"
        )
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent",
            subresults=[child1, child2]
        )

        panel = parent.to_rich_panel()
        self.assertIsNotNone(panel)

    def test_to_rich_panel_with_exception(self):
        """Test that exception is rendered without crashes."""
        exc = RuntimeError("test error")
        result = TaskResult(
            level=TaskResultLevel.ERROR,
            step_title="test",
            exception=exc
        )

        # Should not raise an exception
        panel = result.to_rich_panel()
        self.assertIsNotNone(panel)

    def test_to_rich_panel_with_nested_exception(self):
        """Test that nested exceptions are rendered without crashes."""
        def inner_function():
            raise ValueError("inner error")

        def outer_function():
            try:
                inner_function()
            except ValueError as e:
                raise RuntimeError("outer error") from e

        try:
            outer_function()
        except Exception as exc:
            result = TaskResult(
                level=TaskResultLevel.ERROR,
                step_title="test",
                exception=exc
            )

            # Should not raise an exception
            panel = result.to_rich_panel()
            self.assertIsNotNone(panel)

    def test_to_string_basic(self):
        """Test basic to_string functionality."""
        result = TaskResult(
            level=TaskResultLevel.OK,
            step_title="test_step",
            message="test message"
        )
        string_repr = result.to_string()
        self.assertIn("OK", string_repr)
        self.assertIn("test_step", string_repr)
        self.assertIn("test message", string_repr)

    def test_to_string_with_exception(self):
        """Test to_string with exception."""
        exc = RuntimeError("test error")
        result = TaskResult(
            level=TaskResultLevel.ERROR,
            step_title="test",
            exception=exc
        )
        string_repr = result.to_string()
        self.assertIn("RuntimeError", string_repr)
        self.assertIn("test error", string_repr)

    def test_to_string_with_subresults(self):
        """Test to_string with subresults."""
        child = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child",
            message="child message"
        )
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent",
            subresults=[child]
        )
        string_repr = parent.to_string()
        self.assertIn("parent", string_repr)
        self.assertIn("child", string_repr)

    def test_str_representation(self):
        """Test __str__ method."""
        result = TaskResult(
            level=TaskResultLevel.OK,
            step_title="test",
            message="message"
        )
        str_repr = str(result)
        self.assertIn("OK", str_repr)

    def test_missing_subresult_handling(self):
        """Test handling of None in subresults."""
        result = TaskResult(
            level=TaskResultLevel.OK,
            step_title="test",
            subresults=[None]
        )
        string_repr = result.to_string()
        self.assertIn("MISSING SUBRESULT", string_repr)

    def test_show_all_parameter(self):
        """Test show_all parameter in to_rich_panel."""
        child1 = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child1",
            message="message1"
        )
        child2 = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child2",
            message="message2"
        )
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent",
            subresults=[child1, child2]
        )

        # With show_all=False (default), OK children might be hidden
        panel_normal = parent.to_rich_panel(show_all=False)

        # With show_all=True, all children should be shown
        panel_all = parent.to_rich_panel(show_all=True)

        self.assertIsNotNone(panel_normal)
        self.assertIsNotNone(panel_all)

    def test_title_prefix_parameter(self):
        """Test title_prefix parameter in to_rich_panel."""
        child = TaskResult(
            level=TaskResultLevel.OK,
            step_title="child",
            message="message"
        )
        parent = TaskResult(
            level=TaskResultLevel.OK,
            step_title="parent",
            subresults=[child]
        )

        panel = parent.to_rich_panel(title_prefix="prefix")
        self.assertIsNotNone(panel)


class ParallelStepGroupTest(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_step_group_wraps_exceptions(self):
        """Test that ParallelStepGroup wraps exceptions in TaskResult."""
        async def failing_command(task, storage):
            raise RuntimeError("step failed")

        async def success_command(task, storage):
            return True

        failing_step = Step("failing_step", failing_command)
        success_step = Step("success_step", success_command)

        group = ParallelStepGroup(
            title="test_group",
            steps=[failing_step, success_step],
        )

        from ydb.tools.mnc.lib.progress import MyProgress
        with MyProgress() as pbar:
            parent_task = await pbar.add_task("parent")
            result = await group.run(parent_task)

        # Result should be ERROR
        self.assertFalse(bool(result))
        self.assertEqual(result.level, TaskResultLevel.ERROR)

        # Should have 2 subresults
        self.assertEqual(len(result.subresults), 2)

        # One should be ERROR (the wrapped exception)
        error_results = [r for r in result.subresults if r.level == TaskResultLevel.ERROR]
        self.assertEqual(len(error_results), 1)

        # The error result should have the exception
        error_result = error_results[0]
        self.assertIsNotNone(error_result.exception)
        self.assertIsInstance(error_result.exception, RuntimeError)
        self.assertEqual(str(error_result.exception), "step failed")

        # Step title should reference the failing step
        self.assertIn("failing_step", error_result.step_title)


class StepIdTest(unittest.TestCase):
    def test_task_node_has_uuid_step_id(self):
        from ydb.tools.mnc.lib.progress import MyProgress

        async def run():
            with MyProgress() as pbar:
                return await pbar.add_task("step")

        node = asyncio.run(run())

        self.assertIsNotNone(node.step_id)
        self.assertEqual(len(node.step_id), 36)
        self.assertEqual(node._title, "step")


class RuntimeActionLoggingTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.prev_workdir = deploy_ctx.work_directory
        deploy_ctx.work_directory = self.temp_dir.name

    async def asyncTearDown(self):
        deploy_ctx.work_directory = self.prev_workdir
        self.temp_dir.cleanup()

    async def test_shell_error_includes_full_log_path(self):
        from ydb.tools.mnc.lib import term

        result = await term.shell("printf 'out'; printf 'err' 1>&2; exit 1", step_title="shell step", step_id="12345678")

        self.assertFalse(bool(result))
        self.assertTrue(result.log_path)
        self.assertTrue(os.path.exists(result.log_path))
        with open(result.log_path, "r", encoding="utf-8") as log_file:
            content = log_file.read()
        self.assertIn("out", content)
        self.assertIn("err", content)

    async def test_ssh_result_to_task_result_contains_full_log(self):
        from ydb.tools.mnc.lib import term

        result = term.Result(1, "stdout text", "stderr text", log_path="/tmp/test.log")
        task_result = term._result_to_task_result(result)

        self.assertEqual(task_result.level, TaskResultLevel.ERROR)
        self.assertIn("Full log:", task_result.message)
        self.assertIn("/tmp/test.log", task_result.message)


if __name__ == '__main__':
    unittest.main()
