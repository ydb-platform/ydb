import argparse
import io
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli.commands import disks as cli_disks
from ydb.tools.mnc.cli.commands import install
from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.cli.ut.helpers import Console, MyProgress, ParentTask, RunStepsResult


class InstallCommandTest(unittest.IsolatedAsyncioTestCase):
    def config(self, sector_map_use="never", domain=None):
        return {
            "sector_map": {"use": sector_map_use},
            "disk_size": 10,
            "build_args": [],
            "domain": domain,
        }

    def step_titles(self, step_group):
        return [step.title for step in step_group.steps]

    def patch_get_machines(self, hosts):
        async def get_machines(config):
            return hosts

        return mock.patch.object(install.common, "get_machines", get_machines)

    async def test_do_returns_act_result(self):
        args = types.SimpleNamespace(
            config=self.config(),
            waiting=5,
            bin_path="/bin/ydb",
            do_not_init=True,
            ignore_failed_stop=True,
        )

        async def act(hosts, config, waiting=None, bin_path=None, do_not_init=None, ignore_failed_stop=None, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertEqual(waiting, 5)
            self.assertEqual(bin_path, "/bin/ydb")
            self.assertTrue(do_not_init)
            self.assertTrue(ignore_failed_stop)
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(install, "act", act):
            self.assertFalse(await install.do(args))

    async def test_act_returns_bool_from_progress_result(self):
        console = Console()
        calls = []

        def make_install_steps(hosts, config, waiting, do_not_init, ignore_failed_stop):
            calls.append((hosts, config, waiting, do_not_init, ignore_failed_stop))
            return "install-step"

        async def run_steps(steps, progress=None, title=None):
            self.assertEqual(steps, ["install-step"])
            self.assertEqual(title, "[bold]Install[/]")
            return RunStepsResult(False)

        with mock.patch.object(install, "make_install_steps", make_install_steps), \
                mock.patch.object(install.progress, "MyProgress", MyProgress), \
                mock.patch.object(install.progress, "run_steps", run_steps):
            self.assertFalse(await install.act(["host1"], self.config(), waiting=7, do_not_init=True, ignore_failed_stop=True, console=console))

        self.assertEqual(calls, [(["host1"], self.config(), 7, True, True)])
        self.assertEqual(console.printed, ["panel"])

    async def test_act_updates_bin_path(self):
        async def run_steps(steps, progress=None, title=None):
            return RunStepsResult(True)

        with mock.patch.object(install.deploy_ctx, "update_path_to_bin") as update_path_to_bin, \
                mock.patch.object(install, "make_install_steps", lambda *args, **kwargs: "install-step"), \
                mock.patch.object(install.progress, "MyProgress", MyProgress), \
                mock.patch.object(install.progress, "run_steps", run_steps):
            self.assertTrue(await install.act(["host1"], self.config(), bin_path="/tmp/ydb", console=Console()))

        update_path_to_bin.assert_called_once_with("/tmp/ydb")

    def test_make_install_steps_passes_ignore_failed_stop(self):
        calls = []

        def make_uninstall_steps(hosts, config, ignore_failed_stop=False):
            calls.append((hosts, config, ignore_failed_stop))
            return types.SimpleNamespace(title="uninstall")

        with mock.patch.object(install.deploy_ctx, "do_rebuild", False), \
                mock.patch.object(install.deploy_ctx, "do_redeploy_bin", False), \
                mock.patch.object(install.uninstall, "make_uninstall_steps", make_uninstall_steps):
            install.make_install_steps(["host1"], self.config(sector_map_use="always"), waiting=1, do_not_init=True, ignore_failed_stop=True)

        self.assertEqual(calls, [(["host1"], self.config(sector_map_use="always"), True)])

    def test_install_uses_cli_disks_commands(self):
        self.assertIs(install.disks, cli_disks)

    def test_stripped_bin_path_is_idempotent(self):
        self.assertEqual(
            install.deploy_ctx.get_stripped_bin_path("/tmp/ydb"),
            "/tmp/ydb_stripped",
        )
        self.assertEqual(
            install.deploy_ctx.get_stripped_bin_path("/tmp/ydb_stripped"),
            "/tmp/ydb_stripped",
        )

    async def test_disk_steps_call_cli_disks_commands(self):
        calls = []
        config = self.config()

        async def act_split(hosts, cfg, part_size=None):
            calls.append(("split", hosts, cfg, str(part_size)))
            return True

        async def act_obliterate(hosts, cfg):
            calls.append(("obliterate", hosts, cfg))
            return True

        with mock.patch.object(install.disks, "act_split", act_split), \
                mock.patch.object(install.disks, "act_obliterate", act_obliterate):
            self.assertTrue(await install.make_split_disks_step(["host1"], config).run(ParentTask()))
            self.assertTrue(await install.make_format_disks_step(["host1"], config).run(ParentTask()))

        self.assertEqual(calls, [
            ("split", ["host1"], config, "10.00GB"),
            ("obliterate", ["host1"], config),
        ])

    def test_make_install_steps_skips_disk_steps_when_sector_map_always(self):
        with mock.patch.object(install.deploy_ctx, "do_rebuild", False), \
                mock.patch.object(install.deploy_ctx, "do_redeploy_bin", False):
            steps = install.make_install_steps(
                ["host1"],
                self.config(sector_map_use="always"),
                waiting=1,
                do_not_init=True,
                ignore_failed_stop=False,
            )

        titles = self.step_titles(steps)
        self.assertNotIn("[bold blue]Split disks", titles)
        self.assertNotIn("[bold blue]Format disks", titles)
        self.assertIn("[bold blue]Generate configs", titles)
        self.assertIn("[bold blue]Install multinode", titles)

    def test_make_install_steps_stops_before_init_when_do_not_init(self):
        with mock.patch.object(install.deploy_ctx, "do_rebuild", False), \
                mock.patch.object(install.deploy_ctx, "do_redeploy_bin", False):
            steps = install.make_install_steps(
                ["host1"],
                self.config(sector_map_use="always", domain={"name": "root"}),
                waiting=1,
                do_not_init=True,
                ignore_failed_stop=False,
            )

        titles = self.step_titles(steps)
        self.assertNotIn("[bold blue]Waiting[/] [yellow]1s[/]", titles)
        self.assertNotIn("[bold blue]Init static", titles)
        self.assertNotIn("[bold blue]Init dynamic", titles)
        self.assertEqual(titles[-1], "[bold blue]start multinode[/]")

    async def test_service_host_filters_and_batches_processes(self):
        calls = []
        parent_task = ParentTask()

        async def get_processes(host):
            self.assertEqual(host, "host1")
            return ["ydb_node_static_1", "ydb_node_dynamic_1", "ydb_node_static_2"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            calls.append((host, operation, batch))
            return True

        with mock.patch.object(install.service, "get_processes", get_processes), \
                mock.patch.object(install.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertTrue(await install.service_host("host1", "start", "static", batch_size=1, parent_task=parent_task))

        self.assertEqual(calls, [
            ("host1", "start", ["ydb_node_static_1"]),
            ("host1", "start", ["ydb_node_static_2"]),
        ])
        self.assertEqual(parent_task.updates, [{"total": 2}, {"advance": 1}, {"advance": 1}])

    async def test_service_host_returns_error_when_agent_operation_fails(self):
        parent_task = ParentTask()

        async def get_processes(host):
            return ["ydb_node_static_1"]

        async def cmd_agent_ydb_operation(host, operation, batch):
            return False

        with mock.patch.object(install.service, "get_processes", get_processes), \
                mock.patch.object(install.service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            result = await install.service_host("host1", "start", "static", parent_task=parent_task)

        self.assertIsInstance(result, progress.TaskResult)
        self.assertFalse(result)
        self.assertIn("Failed to start static nodes on host1", result.message)

    def test_install_argparse_rejects_removed_without_test_install_flag(self):
        parser = argparse.ArgumentParser()
        install.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["--without-test-install"])
