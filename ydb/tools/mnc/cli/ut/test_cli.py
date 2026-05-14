import argparse
import io
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import main
from ydb.tools.mnc.cli.commands import disks
from ydb.tools.mnc.lib import agent_client
from ydb.tools.mnc.lib.exceptions import CliError


class CliMainTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_main_raises_cli_error_when_command_returns_false(self):
        async def do(args):
            return False

        module = types.SimpleNamespace(
            __name__="ydb.tools.mnc.cli.commands.fail",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with mock.patch.object(main, "modules", [module]), mock.patch("sys.argv", ["mnc", "fail"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'fail' failed")


class DisksCommandTest(unittest.IsolatedAsyncioTestCase):
    async def test_check_returns_false_on_failed_disk_check(self):
        args = types.SimpleNamespace(config={"disks": []})

        async def get_machines(config):
            return ["host1"]

        async def act_check(hosts, config):
            self.assertEqual(hosts, ["host1"])
            return False

        with mock.patch.object(disks.common, "get_machines", get_machines), mock.patch.object(disks, "act_check", act_check):
            self.assertFalse(await disks.do_check(args))

    async def test_do_dispatches_selected_command_result(self):
        args = types.SimpleNamespace(cmd="info")

        async def do_info(args):
            return True

        with mock.patch.object(disks, "do_info", do_info):
            self.assertTrue(await disks.do(args))

    def test_split_requires_part_count_or_part_size(self):
        parser = argparse.ArgumentParser()
        disks.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["split"])

        args = parser.parse_args(["split", "--part_count", "2"])
        self.assertEqual(args.cmd, "split")
        self.assertEqual(args.part_count, 2)


class AgentClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_post_json_with_parent_task_returns_response_from_step(self):
        class Task:
            async def update(self, **kwargs):
                pass

        class ParentTask:
            async def add_subtask(self, *args, **kwargs):
                return Task()

        async def post_json(host, path, payload, port):
            self.assertEqual((host, path, payload, port), ("host1", "/path", {"key": "value"}, 8999))
            return {"success": True}

        with mock.patch.object(agent_client, "_post_json", post_json):
            response = await agent_client.post_json("host1", "/path", {"key": "value"}, parent_task=ParentTask())

        self.assertEqual(response, {"success": True})
