import argparse
import io
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import main
from ydb.tools.mnc.cli.commands import disks
from ydb.tools.mnc.lib import agent_client
from ydb.tools.mnc.lib.exceptions import CliError


class Response:
    def __init__(self, status=200, payload=None, text="error", json_exception=None):
        self.status = status
        self.payload = payload or {}
        self.text_value = text
        self.json_exception = json_exception

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def text(self):
        return self.text_value

    async def json(self):
        if self.json_exception is not None:
            raise self.json_exception
        return self.payload


class ClientSession:
    def __init__(self, response):
        self.response = response
        self.post_calls = []
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def post(self, url, json):
        self.post_calls.append((url, json))
        return self.response

    def get(self, url):
        self.get_calls.append(url)
        return self.response


class Task:
    def __init__(self):
        self.updates = []

    async def update(self, **kwargs):
        self.updates.append(kwargs)


class ParentTask(Task):
    def __init__(self):
        super().__init__()
        self.subtasks = []

    async def add_subtask(self, *args, **kwargs):
        task = Task()
        self.subtasks.append((args, kwargs, task))
        return task


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
                await disks.info("host1", self.disk_config()["disks"][0]),
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
                await disks.info("host1", self.disk_config()["disks"][0]),
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


class AgentClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_post_json_with_parent_task_returns_response_from_step(self):
        async def post_json(host, path, payload, port):
            self.assertEqual((host, path, payload, port), ("host1", "/path", {"key": "value"}, 8999))
            return {"success": True}

        with mock.patch.object(agent_client, "_post_json", post_json):
            response = await agent_client.post_json("host1", "/path", {"key": "value"}, parent_task=ParentTask())

        self.assertEqual(response, {"success": True})

    async def test_post_json_with_parent_task_returns_none_on_step_failure(self):
        parent_task = ParentTask()

        async def post_json(host, path, payload, port):
            return None

        with mock.patch.object(agent_client, "_post_json", post_json):
            response = await agent_client.post_json("host1", "/path", {}, parent_task=parent_task)

        self.assertIsNone(response)
        self.assertEqual(parent_task.subtasks[0][2].updates, [])

    async def test_post_json_returns_none_on_http_error(self):
        session = ClientSession(Response(status=500, text="boom"))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            self.assertIsNone(await agent_client._post_json("host1", "/path", {"key": "value"}))

        self.assertEqual(session.post_calls, [("http://host1:8999/path", {"key": "value"})])

    async def test_post_json_returns_none_on_invalid_json(self):
        session = ClientSession(Response(status=200, json_exception=ValueError("bad json")))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            self.assertIsNone(await agent_client._post_json("host1", "/path", {}))

    async def test_health_check_succeeds_when_required_features_enabled(self):
        session = ClientSession(Response(payload={"enabled_features": ["nodes", "disks"]}))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            self.assertTrue(await agent_client.CheckAgentHealthOnHost("host1").action())

        self.assertEqual(session.get_calls, ["http://host1:8999/health"])

    async def test_health_check_fails_on_missing_features(self):
        session = ClientSession(Response(payload={"enabled_features": ["nodes"]}))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            result = await agent_client.CheckAgentHealthOnHost("host1").action()

        self.assertFalse(result)
        self.assertIn("does not support features: disks", result.message)

    async def test_health_check_returns_false_on_http_error(self):
        session = ClientSession(Response(status=500))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            self.assertFalse(await agent_client.CheckAgentHealthOnHost("host1").action())
