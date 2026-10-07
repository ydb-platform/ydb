import json
import unittest
from dataclasses import asdict
from unittest import mock

from ydb.tools.mnc.agent.api import nodes as nodes_api
from ydb.tools.mnc.agent.schemas.node import NodeStatusSchema, NodesResponseSchema


class FakeRequest:
    def __init__(self, payload=None, query=None, match_info=None):
        self.payload = payload or {}
        self.query = query or {}
        self.match_info = match_info or {}

    async def json(self):
        return self.payload


class Query:
    def __init__(self, values):
        self.values = values

    def getall(self, name, default):
        return self.values.get(name, default)


class ApiNodesTest(unittest.IsolatedAsyncioTestCase):
    async def add_task(self, task):
        self.added_task = task

    async def call_task_handler(self, handler, payload):
        with mock.patch.object(nodes_api.task_service, "add_task", self.add_task):
            response = await handler(FakeRequest(payload=payload))
        return response

    def response_json(self, response):
        return json.loads(response.text)

    async def test_start_returns_task_id_and_parses_request(self):
        response = await self.call_task_handler(
            nodes_api.start_batch_nodes,
            {"nodes": ["n1"], "timeout": 12, "wait": True, "force": True},
        )

        self.assertEqual(self.response_json(response), {"task_id": self.added_task.task_id, "status": "pending"})
        self.assertEqual(self.added_task.operation, "start")
        self.assertEqual(self.added_task.request.nodes, ["n1"])
        self.assertEqual(self.added_task.request.timeout, 12)
        self.assertTrue(self.added_task.request.wait)
        self.assertTrue(self.added_task.request.force)

    async def test_stop_restart_enable_disable_return_task_id(self):
        handlers = [
            (nodes_api.stop_batch_nodes, "stop"),
            (nodes_api.restart_batch_nodes, "restart"),
            (nodes_api.enable_batch_nodes, "enable"),
            (nodes_api.disable_batch_nodes, "disable"),
        ]

        for handler, operation in handlers:
            response = await self.call_task_handler(handler, {"nodes": ["n1"]})

            self.assertEqual(self.response_json(response)["task_id"], self.added_task.task_id)
            self.assertEqual(self.added_task.operation, operation)

    async def test_uninstall_uses_positive_stop_flag(self):
        response = await self.call_task_handler(
            nodes_api.uninstall_batch_nodes,
            {"nodes": ["n1"], "timeout": 5, "stop": False},
        )

        self.assertEqual(self.response_json(response)["task_id"], self.added_task.task_id)
        self.assertEqual(self.added_task.operation, "uninstall")
        self.assertFalse(self.added_task.request.stop)
        self.assertEqual(self.added_task.request.timeout, 5)

    async def test_install_returns_task_id(self):
        response = await self.call_task_handler(nodes_api.install_nodes, {"yaml_config": "config"})

        self.assertEqual(self.response_json(response)["task_id"], self.added_task.task_id)
        self.assertEqual(self.added_task.request.yaml_config, "config")

    async def test_get_nodes_returns_service_response(self):
        expected = NodesResponseSchema(nodes=[NodeStatusSchema(node="n1", running=True, enabled=True)])

        async def get_nodes():
            return expected

        with mock.patch.object(nodes_api.nodes_service, "get_nodes", get_nodes):
            response = await nodes_api.get_nodes(FakeRequest())

        self.assertEqual(self.response_json(response), asdict(expected))

    async def test_get_nodes_returns_fallback_on_service_error(self):
        async def get_nodes():
            raise RuntimeError("boom")

        with mock.patch.object(nodes_api.nodes_service, "get_nodes", get_nodes):
            response = await nodes_api.get_nodes(FakeRequest())

        self.assertEqual(
            self.response_json(response),
            {
                "error": "Failed to get nodes",
                "message": "boom",
                "nodes": [],
            },
        )

    async def test_status_reads_query_node_list(self):
        calls = []

        async def status_node(node):
            calls.append(node)
            return NodeStatusSchema(node=node, running=False)

        request = FakeRequest(query=Query({"node": ["n1", "n2"]}))
        with mock.patch.object(nodes_api.nodes_service, "status_node", status_node):
            response = await nodes_api.get_batch_nodes_status(request)

        self.assertEqual(calls, ["n1", "n2"])
        self.assertEqual(
            self.response_json(response),
            {"nodes": [asdict(NodeStatusSchema(node="n1", running=False)), asdict(NodeStatusSchema(node="n2", running=False))]},
        )

    def test_mutating_node_get_routes_are_not_registered(self):
        get_paths = set()
        post_paths = set()
        for route_def in nodes_api.routes._items:
            if route_def.method == "GET":
                get_paths.add(route_def.path)
            if route_def.method == "POST":
                post_paths.add(route_def.path)

        mutating = {
            "/nodes/start",
            "/nodes/stop",
            "/nodes/restart",
            "/nodes/enable",
            "/nodes/disable",
            "/nodes/uninstall",
            "/nodes/install",
        }
        self.assertFalse(mutating & get_paths)
        self.assertTrue(mutating <= post_paths)
