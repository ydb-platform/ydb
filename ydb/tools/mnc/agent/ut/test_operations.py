import unittest
from dataclasses import asdict
from unittest import mock

from ydb.tools.mnc.agent.schemas.disk import DiskDeviceSchema, DiskOperationItemSchema, DiskOperationRequest, DiskOperationResponse
from ydb.tools.mnc.agent.schemas.node import (
    InstallNodesRequest,
    InstallNodesResponse,
    NodeOperationRequest,
    NodeServiceOperationBatchSchema,
    NodeServiceOperationSchema,
    UninstallNodesRequest,
)
from ydb.tools.mnc.agent.services import operations


class OperationsTest(unittest.IsolatedAsyncioTestCase):
    def batch(self, success=True):
        return NodeServiceOperationBatchSchema(
            operations=[
                NodeServiceOperationSchema(
                    node="ydb_node_static_1",
                    operation="start_node",
                    success=success,
                    message="ok" if success else "failed",
                )
            ]
        )

    async def test_node_start_task_calls_service_and_returns_data(self):
        request = NodeOperationRequest(nodes=["ydb_node_static_1"], timeout=7, wait=True, force=True)

        async def start_nodes(nodes, timeout=10, wait=False, force=False):
            self.assertEqual(nodes, ["ydb_node_static_1"])
            self.assertEqual(timeout, 7)
            self.assertTrue(wait)
            self.assertTrue(force)
            return self.batch()

        with mock.patch.object(operations.nodes_service, "start_nodes", start_nodes):
            result = await operations.NodeOperationTask("start", request).do()

        self.assertTrue(result.success)
        self.assertEqual(result.data, asdict(self.batch()))

    async def test_node_operation_task_reports_per_operation_failure(self):
        async def stop_nodes(nodes, timeout=10, wait=False):
            return self.batch(success=False)

        with mock.patch.object(operations.nodes_service, "stop_nodes", stop_nodes):
            result = await operations.NodeOperationTask("stop", NodeOperationRequest(nodes=["n"])).do()

        self.assertFalse(result.success)
        self.assertEqual(result.data["operations"][0]["message"], "failed")

    async def test_node_restart_task_passes_force(self):
        calls = []

        async def restart_nodes(nodes, timeout=10, wait=False, force=False):
            calls.append((nodes, timeout, wait, force))
            return self.batch()

        request = NodeOperationRequest(nodes=["n"], timeout=11, wait=False, force=True)
        with mock.patch.object(operations.nodes_service, "restart_nodes", restart_nodes):
            await operations.NodeOperationTask("restart", request).do()

        self.assertEqual(calls, [(["n"], 11, False, True)])

    async def test_node_enable_task_calls_enable_for_each_node(self):
        calls = []

        async def enable_node(node):
            calls.append(node)
            return NodeServiceOperationSchema(node=node, operation="enable_node", success=True)

        request = NodeOperationRequest(nodes=["n1", "n2"])
        with mock.patch.object(operations.nodes_service, "enable_node", enable_node):
            result = await operations.NodeEnableTask(True, request).do()

        self.assertEqual(calls, ["n1", "n2"])
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["operations"]), 2)

    async def test_node_disable_task_reports_failure(self):
        async def disable_node(node):
            return NodeServiceOperationSchema(node=node, operation="disable_node", success=False, message="no")

        with mock.patch.object(operations.nodes_service, "disable_node", disable_node):
            result = await operations.NodeEnableTask(False, NodeOperationRequest(nodes=["n"])).do()

        self.assertFalse(result.success)
        self.assertEqual(result.data["operations"][0]["message"], "no")

    async def test_node_uninstall_task_uses_stop_flag(self):
        calls = []

        async def uninstall_nodes(nodes, timeout=10, stop=True):
            calls.append((nodes, timeout, stop))
            return self.batch()

        request = UninstallNodesRequest(nodes=["n"], timeout=9, stop=False)
        with mock.patch.object(operations.nodes_service, "uninstall_nodes", uninstall_nodes):
            await operations.NodeUninstallTask(request).do()

        self.assertEqual(calls, [(["n"], 9, False)])

    async def test_node_install_task_success_and_failure(self):
        async def install_ok(request):
            return InstallNodesResponse(nodes=["n"])

        async def install_fail(request):
            return InstallNodesResponse(error="already installed")

        request = InstallNodesRequest(yaml_config="config")
        with mock.patch.object(operations.nodes_service, "install_nodes", install_ok):
            ok = await operations.NodeInstallTask(request).do()
        with mock.patch.object(operations.nodes_service, "install_nodes", install_fail):
            failed = await operations.NodeInstallTask(request).do()

        self.assertTrue(ok.success)
        self.assertEqual(ok.data, {"nodes": ["n"], "error": None})
        self.assertFalse(failed.success)
        self.assertEqual(failed.message, "already installed")

    async def test_disk_operation_task_calls_service(self):
        request = DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
        response = DiskOperationResponse(
            success=True,
            operations=[DiskOperationItemSchema(partlabel="label1", success=True, message="ok")],
        )

        async def split(req):
            self.assertIs(req, request)
            return response

        with mock.patch.object(operations.disk_service, "split", split):
            result = await operations.DiskOperationTask("split", request).do()

        self.assertTrue(result.success)
        self.assertEqual(result.data, asdict(response))

    async def test_disk_operation_task_reports_failed_operation(self):
        response = DiskOperationResponse(
            success=False,
            operations=[DiskOperationItemSchema(partlabel="label1", success=False, message="bad")],
        )

        async def obliterate(request):
            return response

        with mock.patch.object(operations.disk_service, "obliterate", obliterate):
            result = await operations.DiskOperationTask("obliterate", DiskOperationRequest()).do()

        self.assertFalse(result.success)
        self.assertEqual(result.data["operations"][0]["message"], "bad")
