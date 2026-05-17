import argparse
import io
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli.commands import disks
from ydb.tools.mnc.cli.ut.helpers import ParentTask


class DisksCommandTest(unittest.IsolatedAsyncioTestCase):
    def disk_config(self):
        return {
            "hosts": ["host1"],
            "disks": [{
                "hosts": ["host1"],
                "disks_for_split": [{"partlabel": "label1", "device": "/dev/sdb"}],
                "disks_for_use": [{"partlabel": "label2"}],
            }],
        }

    def patch_get_machines(self, hosts):
        async def get_machines(config):
            return hosts

        return mock.patch.object(disks.common, "get_machines", get_machines)

    async def test_check_returns_false_on_failed_disk_check(self):
        args = types.SimpleNamespace(config={"disks": []})

        async def act_check(hosts, config):
            self.assertEqual(hosts, ["host1"])
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(disks, "act_check", act_check):
            self.assertFalse(await disks.do_check(args))

    async def test_do_split_returns_act_split_result(self):
        args = types.SimpleNamespace(config={}, part_count=2, part_size=None)

        async def act_split(hosts, config, part_count=None, part_size=None):
            self.assertEqual(hosts, ["host1"])
            self.assertEqual(part_count, 2)
            self.assertIsNone(part_size)
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(disks, "act_split", act_split):
            self.assertFalse(await disks.do_split(args))

    async def test_do_unite_returns_act_unite_result(self):
        args = types.SimpleNamespace(config={})

        async def act_unite(hosts, config):
            self.assertEqual(hosts, ["host1"])
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(disks, "act_unite", act_unite):
            self.assertFalse(await disks.do_unite(args))

    async def test_do_obliterate_returns_act_obliterate_result(self):
        args = types.SimpleNamespace(config={})

        async def act_obliterate(hosts, config):
            self.assertEqual(hosts, ["host1"])
            return False

        with self.patch_get_machines(["host1"]), mock.patch.object(disks, "act_obliterate", act_obliterate):
            self.assertFalse(await disks.do_obliterate(args))

    async def test_do_info_returns_false_when_agent_info_fails(self):
        args = types.SimpleNamespace(config={})

        async def act_info(hosts, config):
            self.assertEqual(hosts, ["host1"])
            return [["host1", disks.INFO_AGENT_FAILURE_MESSAGE]]

        with self.patch_get_machines(["host1"]), \
                mock.patch.object(disks, "act_info", act_info), \
                mock.patch("builtins.print"):
            self.assertFalse(await disks.do_info(args))

    async def test_do_dispatches_all_command_results(self):
        commands = ("check", "info", "split", "unite", "obliterate")
        called = []

        def make_handler(command):
            async def handler(args):
                called.append(command)
                return command != "split"

            return handler

        patches = [
            mock.patch.object(disks, "do_check", make_handler("check")),
            mock.patch.object(disks, "do_info", make_handler("info")),
            mock.patch.object(disks, "do_split", make_handler("split")),
            mock.patch.object(disks, "do_unite", make_handler("unite")),
            mock.patch.object(disks, "do_obliterate", make_handler("obliterate")),
        ]
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            results = [await disks.do(types.SimpleNamespace(cmd=command)) for command in commands]

        self.assertEqual(called, list(commands))
        self.assertEqual(results, [True, True, False, True, True])

    def test_split_requires_part_count_or_part_size(self):
        parser = argparse.ArgumentParser()
        disks.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["split"])

        args = parser.parse_args(["split", "--part_count", "2"])
        self.assertEqual(args.cmd, "split")
        self.assertEqual(args.part_count, 2)

    def test_split_rejects_part_count_and_part_size_together(self):
        parser = argparse.ArgumentParser()
        disks.add_arguments(parser)

        with mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["split", "--part_count", "2", "--part-size", "10GB"])

    async def test_check_devices_returns_false_when_agent_returns_none(self):
        async def post_json(host, path, payload):
            self.assertEqual(host, "host1")
            self.assertEqual(path, "/disks/check")
            self.assertEqual(payload, {
                "disks_for_split": [{"partlabel": "label1", "device": "/dev/sdb"}],
                "disks_for_use": [{"partlabel": "label2", "device": None}],
            })
            return None

        disk_config = {
            "disks_for_split": [{"partlabel": "label1", "device": "/dev/sdb"}],
            "disks_for_use": [{"partlabel": "label2"}],
        }
        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.check_devices("host1", disk_config))

    async def test_split_sends_expected_payload(self):
        parent_task = ParentTask()
        devices = [disks.common.Device("label1", "/dev/sdb")]

        async def post_json(host, path, payload, parent_task=None):
            self.assertEqual(host, "host1")
            self.assertEqual(path, "/disks/split")
            self.assertEqual(payload, {
                "devices": [{"partlabel": "label1", "device": "/dev/sdb"}],
                "part_count": None,
                "part_size": "10.00GB",
            })
            self.assertIsNotNone(parent_task)
            return {"success": True, "operations": [{"success": True, "partlabel": "label1"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertTrue(await disks.split("host1", devices, part_size=disks.common.Memory("10GB"), parent_task=parent_task))

    async def test_act_split_does_not_split_when_check_fails(self):
        calls = []

        async def act_check(hosts, config):
            calls.append(("check", hosts, config))
            return False

        async def split(*args, **kwargs):
            calls.append(("split", args, kwargs))
            return True

        config = {"hosts": ["host1"], "disks": []}
        with mock.patch.object(disks, "act_check", act_check), mock.patch.object(disks, "split", split):
            self.assertFalse(await disks.act_split(["host1"], config, parent_task=ParentTask()))

        self.assertEqual(calls, [("check", ["host1"], config)])

    async def test_act_check_uses_agent_response(self):
        calls = []

        async def post_json(host, path, payload, **kwargs):
            calls.append((host, path, payload, kwargs))
            return {
                "success": True,
                "checks": [{"success": True, "partlabel": "label1"}],
            }

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertTrue(await disks.act_check(["host1"], self.disk_config()))

        self.assertEqual(calls, [(
            "host1",
            "/disks/check",
            {
                "disks_for_split": [{"partlabel": "label1", "device": "/dev/sdb"}],
                "disks_for_use": [{"partlabel": "label2", "device": None}],
            },
            {},
        )])

    async def test_act_check_returns_false_on_agent_failed_check(self):
        async def post_json(host, path, payload, **kwargs):
            return {
                "success": False,
                "checks": [{"success": False, "partlabel": "label1", "message": "bad disk"}],
            }

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.act_check(["host1"], self.disk_config()))

    async def test_info_formats_agent_response(self):
        async def post_json(host, path, payload, **kwargs):
            self.assertEqual(path, "/disks/info")
            self.assertEqual(payload, {})
            return {
                "disks": [{
                    "path": "/dev/sdb",
                    "size": "100.00GB",
                    "parts": [{
                        "id": "1",
                        "from_mem": "0",
                        "to_mem": "10.00GB",
                        "size": "10.00GB",
                        "label": "label1",
                    }],
                }],
            }

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertEqual(
                await disks.info("host1"),
                [
                    "host1",
                    "  /dev/sdb 100.00GB",
                    "    1 0 10.00GB 10.00GB label1",
                ],
            )

    async def test_info_returns_fallback_report_when_agent_fails(self):
        async def post_json(host, path, payload, **kwargs):
            return None

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertEqual(
                await disks.info("host1"),
                ["host1", " failed to receive disk info from agent"],
            )

    async def test_act_split_uses_mock_agent_for_check_and_split(self):
        calls = []

        async def post_json(host, path, payload, **kwargs):
            calls.append((path, payload, bool(kwargs.get("parent_task"))))
            if path == "/disks/check":
                return {"success": True, "checks": [{"success": True, "partlabel": "label1"}]}
            if path == "/disks/split":
                return {"success": True, "operations": [{"success": True, "partlabel": "label1"}]}
            raise AssertionError(path)

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertTrue(await disks.act_split(["host1"], self.disk_config(), part_count=2, parent_task=ParentTask()))

        self.assertEqual(calls, [
            (
                "/disks/check",
                {
                    "disks_for_split": [{"partlabel": "label1", "device": "/dev/sdb"}],
                    "disks_for_use": [{"partlabel": "label2", "device": None}],
                },
                False,
            ),
            (
                "/disks/split",
                {
                    "devices": [{"partlabel": "label1", "device": "/dev/sdb"}],
                    "part_count": 2,
                    "part_size": None,
                },
                True,
            ),
        ])

    async def test_act_split_does_not_call_split_when_agent_check_fails(self):
        calls = []

        async def post_json(host, path, payload, **kwargs):
            calls.append(path)
            return {"success": False, "checks": [{"success": False, "partlabel": "label1"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.act_split(["host1"], self.disk_config(), part_count=2, parent_task=ParentTask()))

        self.assertEqual(calls, ["/disks/check"])

    async def test_act_unite_uses_mock_agent(self):
        calls = []

        async def post_json(host, path, payload, **kwargs):
            calls.append((path, payload, bool(kwargs.get("parent_task"))))
            return {"success": True, "operations": [{"success": True, "partlabel": "label1"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertTrue(await disks.act_unite(["host1"], self.disk_config(), parent_task=ParentTask()))

        self.assertEqual(calls, [(
            "/disks/unite",
            {"devices": [{"partlabel": "label1", "device": "/dev/sdb"}]},
            True,
        )])

    async def test_act_obliterate_uses_mock_agent_for_all_disks(self):
        calls = []

        async def post_json(host, path, payload, **kwargs):
            calls.append((path, payload, bool(kwargs.get("parent_task"))))
            return {
                "success": True,
                "operations": [
                    {"success": True, "partlabel": "label1"},
                    {"success": True, "partlabel": "label2"},
                ],
            }

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertTrue(await disks.act_obliterate(["host1"], self.disk_config(), parent_task=ParentTask()))

        self.assertEqual(calls, [(
            "/disks/obliterate",
            {"devices": [
                {"partlabel": "label2", "device": None},
                {"partlabel": "label1", "device": "/dev/sdb"},
            ]},
            True,
        )])

    async def test_split_returns_false_on_failed_agent_operation(self):
        async def post_json(host, path, payload, **kwargs):
            return {"success": False, "operations": [{"success": False, "partlabel": "label1", "message": "failed"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.split(
                "host1",
                [disks.common.Device("label1", "/dev/sdb")],
                part_count=2,
                parent_task=ParentTask(),
            ))

    async def test_unite_returns_false_on_failed_agent_operation(self):
        async def post_json(host, path, payload, **kwargs):
            return {"success": False, "operations": [{"success": False, "partlabel": "label1", "message": "failed"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.unite(
                "host1",
                [disks.common.Device("label1", "/dev/sdb")],
                parent_task=ParentTask(),
            ))

    async def test_obliterate_returns_false_on_failed_agent_operation(self):
        async def post_json(host, path, payload, **kwargs):
            return {"success": False, "operations": [{"success": False, "partlabel": "label1", "message": "failed"}]}

        with mock.patch.object(disks.agent_client, "post_json", post_json):
            self.assertFalse(await disks.obliterate(
                "host1",
                [disks.common.Device("label1", "/dev/sdb")],
                parent_task=ParentTask(),
            ))
