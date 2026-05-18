import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli.commands import disks as cli_disks
from ydb.tools.mnc.cli.commands import uninstall
from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.cli.ut.helpers import Console, MyProgress, ParentTask, RunStepsResult


class UninstallCommandTest(unittest.IsolatedAsyncioTestCase):
    def config(self, sector_map_use="never"):
        return {
            "sector_map": {"use": sector_map_use},
        }

    def step_titles(self, step_group):
        return [step.title for step in step_group.steps]

    def patch_get_machines(self, hosts):
        async def get_machines(config):
            return hosts

        return mock.patch.object(uninstall.common, "get_machines", get_machines)

    async def test_do_returns_act_result(self):
        args = types.SimpleNamespace(config=self.config(), ignore_failed_stop=True)

        async def act(hosts, config, ignore_failed_stop=False, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertTrue(ignore_failed_stop)
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(uninstall, "act", act):
            self.assertFalse(await uninstall.do(args))

    async def test_act_returns_bool_from_progress_result(self):
        console = Console()
        calls = []

        def make_uninstall_steps(hosts, config, ignore_failed_stop=False):
            calls.append((hosts, config, ignore_failed_stop))
            return "uninstall-step"

        async def run_steps(steps, progress=None, title=None):
            self.assertEqual(steps, ["uninstall-step"])
            self.assertEqual(title, "[bold]Uninstall[/]")
            return RunStepsResult(False)

        with mock.patch.object(uninstall, "make_uninstall_steps", make_uninstall_steps), \
                mock.patch.object(uninstall.progress, "MyProgress", MyProgress), \
                mock.patch.object(uninstall.progress, "run_steps", run_steps):
            self.assertFalse(await uninstall.act(["host1"], self.config(), ignore_failed_stop=True, console=console))

        self.assertEqual(calls, [(["host1"], self.config(), True)])
        self.assertEqual(console.printed, ["panel"])

    def test_uninstall_uses_cli_disks_commands(self):
        self.assertIs(uninstall.disks, cli_disks)

    async def test_make_uninstall_steps_passes_ignore_failed_stop_to_stop_step(self):
        steps = uninstall.make_uninstall_steps(["host1"], self.config(), ignore_failed_stop=True)
        stop_group = steps.steps[1]
        stop_step = stop_group.steps[0]
        parent_task = ParentTask()

        async def stop_host(host, batch_size=10, ignore_failed_stop=False, parent_task=None):
            self.assertEqual(host, "host1")
            self.assertTrue(ignore_failed_stop)
            return True

        with mock.patch.object(uninstall, "stop_host", stop_host):
            result = await stop_step.run(parent_task)

        self.assertTrue(result)

    def test_make_uninstall_steps_has_return_disks_step(self):
        steps = uninstall.make_uninstall_steps(["host1"], self.config())
        self.assertEqual(self.step_titles(steps), [
            "[bold blue]Check agents on hosts",
            "[bold blue]Stop hosts[/]",
            "[bold blue]Uninstall multinode[/]",
            "[bold blue]Return disks[/]",
        ])

    async def test_return_disks_step_skipped_when_sector_map_always(self):
        step = uninstall.make_group_return_disks_step(["host1"], self.config(sector_map_use="always"))
        result = await step.run(ParentTask())

        self.assertEqual(result.level, progress.TaskResultLevel.SKIPPED)

    async def test_return_disks_step_runs_when_sector_map_not_always(self):
        calls = []

        async def act_unite(hosts, config):
            calls.append((hosts, config))
            return True

        step = uninstall.make_group_return_disks_step(["host1"], self.config(sector_map_use="never"))
        with mock.patch.object(uninstall.disks, "act_unite", act_unite):
            result = await step.run(ParentTask())

        self.assertTrue(result)
        self.assertEqual(calls, [(["host1"], self.config(sector_map_use="never"))])

    async def test_stop_host_batches_processes(self):
        calls = []
        parent_task = ParentTask()

        async def get_processes(host):
            self.assertEqual(host, "host1")
            return ["node1", "node2", "node3"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            calls.append((host, operation, batch))
            return True

        with mock.patch.object(uninstall.service, "get_processes", get_processes), \
                mock.patch.object(uninstall.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertTrue(await uninstall.stop_host("host1", batch_size=2, parent_task=parent_task))

        self.assertEqual(calls, [
            ("host1", "stop", ["node1", "node2"]),
            ("host1", "stop", ["node3"]),
        ])
        self.assertEqual(parent_task.updates, [{"total": 3}, {"advance": 2}, {"advance": 1}])

    async def test_stop_host_returns_error_when_stop_fails(self):
        parent_task = ParentTask()

        async def get_processes(host):
            return ["node1"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            return False

        with mock.patch.object(uninstall.service, "get_processes", get_processes), \
                mock.patch.object(uninstall.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            result = await uninstall.stop_host("host1", parent_task=parent_task)

        self.assertIsInstance(result, progress.TaskResult)
        self.assertFalse(result)
        self.assertIn("Failed to stop nodes on host1", result.message)

    async def test_stop_host_returns_warning_when_stop_fails_and_ignored(self):
        parent_task = ParentTask()

        async def get_processes(host):
            return ["node1"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            return False

        with mock.patch.object(uninstall.service, "get_processes", get_processes), \
                mock.patch.object(uninstall.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            result = await uninstall.stop_host("host1", ignore_failed_stop=True, parent_task=parent_task)

        self.assertIsInstance(result, progress.TaskResult)
        self.assertTrue(result)
        self.assertEqual(result.level, progress.TaskResultLevel.WARNING)
        self.assertIn("Ignored failed stop on host1", result.message)

    async def test_uninstall_host_batches_processes(self):
        calls = []
        parent_task = ParentTask()

        async def get_processes(host):
            return ["node1", "node2", "node3"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            calls.append((host, operation, batch))
            return True

        with mock.patch.object(uninstall.service, "get_processes", get_processes), \
                mock.patch.object(uninstall.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertTrue(await uninstall.uninstall_host("host1", batch_size=2, parent_task=parent_task))

        self.assertEqual(calls, [
            ("host1", "uninstall", ["node1", "node2"]),
            ("host1", "uninstall", ["node3"]),
        ])
        self.assertEqual(parent_task.updates, [{"total": 3}, {"advance": 2}, {"advance": 1}])

    async def test_uninstall_host_returns_error_when_uninstall_fails(self):
        parent_task = ParentTask()

        async def get_processes(host):
            return ["node1"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            return False

        with mock.patch.object(uninstall.service, "get_processes", get_processes), \
                mock.patch.object(uninstall.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            result = await uninstall.uninstall_host("host1", parent_task=parent_task)

        self.assertIsInstance(result, progress.TaskResult)
        self.assertFalse(result)
        self.assertIn("Failed to uninstall nodes on host1", result.message)
