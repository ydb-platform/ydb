import unittest
from unittest import mock

from ydb.tools.mnc.lib import agent_client

from ydb.tools.mnc.cli.ut.helpers import ClientSession, ParentTask, Response


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

    async def test_get_json_returns_response_payload(self):
        session = ClientSession(Response(payload={"success": True}))

        with mock.patch.object(agent_client.aiohttp, "ClientSession", lambda: session):
            self.assertEqual(await agent_client._get_json("host1", "/path"), {"success": True})

        self.assertEqual(session.get_calls, ["http://host1:8999/path"])

    async def test_post_json_and_wait_polls_returned_task(self):
        calls = []

        async def post_json(host, path, payload, port=8999, parent_task=None):
            calls.append(("post", host, path, payload, port, parent_task))
            return {"task_id": "task-1"}

        async def wait_task(host, task_id, port=8999):
            calls.append(("wait", host, task_id, port))
            return {"success": True}

        with mock.patch.object(agent_client, "post_json", post_json), \
                mock.patch.object(agent_client, "wait_task", wait_task):
            self.assertEqual(
                await agent_client.post_json_and_wait("host1", "/path", {"key": "value"}, parent_task="task"),
                {"success": True},
            )

        self.assertEqual(calls, [
            ("post", "host1", "/path", {"key": "value"}, 8999, "task"),
            ("wait", "host1", "task-1", 8999),
        ])

    async def test_post_json_and_wait_returns_none_without_task_id(self):
        async def post_json(host, path, payload, port=8999, parent_task=None):
            return {"success": True}

        with mock.patch.object(agent_client, "post_json", post_json):
            self.assertIsNone(await agent_client.post_json_and_wait("host1", "/path", {}))

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
