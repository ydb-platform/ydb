import argparse
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli.commands import agent
from ydb.tools.mnc.cli.ut.helpers import Console, MyProgress, RunStepsResult


class AgentCommandTest(unittest.IsolatedAsyncioTestCase):
    def config(self):
        return {
            "hosts": ["host1"],
            "port": 8998,
            "mnc_home": "/tmp/mnc home",
        }

    def step_titles(self, step_group):
        return [step.title for step in step_group.steps]

    def patch_get_machines(self, hosts):
        async def get_machines(config):
            return hosts

        return mock.patch.object(agent.common, "get_machines", get_machines)

    def test_agent_run_command_includes_target_host_port_and_home(self):
        with mock.patch.object(agent.deploy_ctx, "deploy_path", "/tmp/deploy"):
            self.assertEqual(
                agent._agent_run_command(self.config(), "host1"),
                "/tmp/deploy/mnc_agent/bin/mnc_agent --config /tmp/deploy/mnc_agent/cfg/mnc_agent.yaml --host host1 --port 8998 --mnc-home '/tmp/mnc home'",
            )

    def test_make_uninstall_steps_has_stop_and_remove(self):
        steps = agent.make_uninstall_steps(["host1"])

        self.assertEqual(self.step_titles(steps), [
            "[bold blue]Stop agents[/]",
            "[bold blue]Remove agent files[/]",
        ])

    async def test_do_uninstall_returns_act_uninstall_result(self):
        args = types.SimpleNamespace(config=self.config())

        async def act_uninstall(hosts, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(agent, "act_uninstall", act_uninstall):
            self.assertFalse(await agent.do_uninstall(args))

    def test_make_start_steps_starts_waits_and_checks_health(self):
        steps = agent.make_start_steps(["host1"], self.config(), waiting=4)

        self.assertEqual(self.step_titles(steps), [
            "[bold blue]Start agents[/]",
            "[bold blue]Waiting[/] [yellow]4s[/]",
            "[bold blue]Check agents on hosts",
        ])

    def test_make_stop_steps_stops_agents(self):
        steps = agent.make_stop_steps(["host1"])

        self.assertEqual(self.step_titles(steps), ["[bold blue]Stop agents[/]"])

    def test_make_restart_steps_stops_starts_waits_and_checks_health(self):
        steps = agent.make_restart_steps(["host1"], self.config(), waiting=4)

        self.assertEqual(self.step_titles(steps), [
            "[bold blue]Stop agents[/]",
            "[bold blue]Start agents[/]",
            "[bold blue]Waiting[/] [yellow]4s[/]",
            "[bold blue]Check agents on hosts",
        ])

    async def test_do_start_returns_act_start_result(self):
        args = types.SimpleNamespace(config=self.config(), waiting=5)

        async def act_start(hosts, config, waiting=None, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertEqual(config, self.config())
            self.assertEqual(waiting, 5)
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(agent, "act_start", act_start):
            self.assertFalse(await agent.do_start(args))

    async def test_do_stop_returns_act_stop_result(self):
        args = types.SimpleNamespace(config=self.config())

        async def act_stop(hosts, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(agent, "act_stop", act_stop):
            self.assertFalse(await agent.do_stop(args))

    async def test_do_restart_returns_act_restart_result(self):
        args = types.SimpleNamespace(config=self.config(), waiting=5)

        async def act_restart(hosts, config, waiting=None, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertEqual(config, self.config())
            self.assertEqual(waiting, 5)
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(agent, "act_restart", act_restart):
            self.assertFalse(await agent.do_restart(args))

    def test_make_install_steps_has_build_and_health_check_by_default(self):
        with mock.patch.object(agent.deploy_ctx, "git_ydb_root", "/git/ydb"), \
                mock.patch.object(agent.deploy_ctx, "deploy_path", "/deploy"):
            steps = agent.make_install_steps(
                ["host1"],
                self.config(),
                do_not_build=False,
                do_not_start=False,
                waiting=3,
            )

        titles = self.step_titles(steps)
        self.assertEqual(titles[0], "[bold cyan]build[/] [yellow]ydb/tools/mnc/agent[/]")
        self.assertIn("[bold blue]Start agents[/]", titles)
        self.assertIn("[bold blue]Waiting[/] [yellow]3s[/]", titles)
        self.assertEqual(titles[-1], "[bold blue]Check agents on hosts")

    def test_make_install_steps_can_skip_build_and_start(self):
        with mock.patch.object(agent.deploy_ctx, "git_ydb_root", "/git/ydb"), \
                mock.patch.object(agent.deploy_ctx, "deploy_path", "/deploy"):
            steps = agent.make_install_steps(
                ["host1"],
                self.config(),
                do_not_build=True,
                do_not_start=True,
                waiting=3,
            )

        titles = self.step_titles(steps)
        self.assertNotIn("[bold cyan]build[/] [yellow]ydb/tools/mnc/agent[/]", titles)
        self.assertNotIn("[bold blue]Start agents[/]", titles)
        self.assertNotIn("[bold blue]Check agents on hosts", titles)
        self.assertEqual(titles, [
            "[bold blue]Stop agents[/]",
            "[bold blue]Prepare agent directories[/]",
            "[bold blue]Deploy agent binary[/]",
            "[bold blue]Write agent config[/]",
            "[bold blue]Deploy agent config[/]",
        ])

    async def test_do_install_returns_act_result(self):
        args = types.SimpleNamespace(
            config=self.config(),
            do_not_build=True,
            do_not_start=True,
            waiting=5,
        )

        async def act(hosts, config, do_not_build=False, do_not_start=False, waiting=None, console=None):
            self.assertEqual(hosts, ["host1"])
            self.assertEqual(config, self.config())
            self.assertTrue(do_not_build)
            self.assertTrue(do_not_start)
            self.assertEqual(waiting, 5)
            self.assertIsNotNone(console)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(agent, "act", act):
            self.assertFalse(await agent.do_install(args))

    async def test_act_returns_progress_result(self):
        console = Console()
        calls = []

        def make_install_steps(hosts, config, do_not_build, do_not_start, waiting):
            calls.append((hosts, config, do_not_build, do_not_start, waiting))
            return "agent-install-step"

        async def run_steps(steps, progress=None, title=None):
            self.assertEqual(steps, ["agent-install-step"])
            self.assertEqual(title, "[bold]Install agents[/]")
            return RunStepsResult(False)

        with mock.patch.object(agent, "make_install_steps", make_install_steps), \
                mock.patch.object(agent.progress, "MyProgress", MyProgress), \
                mock.patch.object(agent.progress, "run_steps", run_steps):
            result = await agent.act(
                ["host1"],
                self.config(),
                do_not_build=True,
                do_not_start=True,
                waiting=7,
                console=console,
            )

        self.assertFalse(result)
        self.assertIsInstance(result, RunStepsResult)

        self.assertEqual(calls, [(["host1"], self.config(), True, True, 7)])
        self.assertEqual(console.printed, ["panel"])

    async def test_do_dispatches_uninstall_result(self):
        async def do_uninstall(args):
            return False

        with mock.patch.object(agent, "do_uninstall", do_uninstall):
            self.assertFalse(await agent.do(types.SimpleNamespace(cmd="uninstall")))

    async def test_do_dispatches_start_result(self):
        async def do_start(args):
            return False

        with mock.patch.object(agent, "do_start", do_start):
            self.assertFalse(await agent.do(types.SimpleNamespace(cmd="start")))

    async def test_do_dispatches_stop_result(self):
        async def do_stop(args):
            return False

        with mock.patch.object(agent, "do_stop", do_stop):
            self.assertFalse(await agent.do(types.SimpleNamespace(cmd="stop")))

    async def test_do_dispatches_restart_result(self):
        async def do_restart(args):
            return False

        with mock.patch.object(agent, "do_restart", do_restart):
            self.assertFalse(await agent.do(types.SimpleNamespace(cmd="restart")))

    async def test_do_dispatches_install_result(self):
        async def do_install(args):
            return False

        with mock.patch.object(agent, "do_install", do_install):
            self.assertFalse(await agent.do(types.SimpleNamespace(cmd="install")))

    def test_agent_argparse_install_flags(self):
        parser = argparse.ArgumentParser()
        agent.add_arguments(parser)

        args = parser.parse_args(["install", "--do-not-build", "--do-not-start", "--waiting", "9"])

        self.assertEqual(args.cmd, "install")
        self.assertTrue(args.do_not_build)
        self.assertTrue(args.do_not_start)
        self.assertEqual(args.waiting, 9)

    def test_agent_argparse_uninstall(self):
        parser = argparse.ArgumentParser()
        agent.add_arguments(parser)

        args = parser.parse_args(["uninstall"])

        self.assertEqual(args.cmd, "uninstall")

    def test_agent_argparse_start_stop_restart(self):
        parser = argparse.ArgumentParser()
        agent.add_arguments(parser)

        start_args = parser.parse_args(["start", "--waiting", "9"])
        stop_args = parser.parse_args(["stop"])
        restart_args = parser.parse_args(["restart", "--waiting", "8"])

        self.assertEqual(start_args.cmd, "start")
        self.assertEqual(start_args.waiting, 9)
        self.assertEqual(stop_args.cmd, "stop")
        self.assertEqual(restart_args.cmd, "restart")
        self.assertEqual(restart_args.waiting, 8)
