import asyncio
import unittest
from unittest import mock

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.schemas.node import (
    DynamicNodeParams,
    InstallNodesRequest,
    NodeStatusSchema,
    NodesResponseSchema,
    StaticNodeParams,
)
from ydb.tools.mnc.agent.services import nodes
from ydb.tools.mnc.lib import term


class PersistentMock:
    def __init__(self):
        self.saved = []
        self.deleted = []
        self.enabled_calls = []

    async def save_node_info(self, node, pid, enabled=True):
        self.saved.append((node, pid, enabled))

    async def set_node_enabled(self, node, enabled):
        self.enabled_calls.append((node, enabled))

    async def delete_node_info(self, node):
        self.deleted.append(node)


class FakeProcess:
    def __init__(self, pid=1000, returncode=None):
        self.pid = pid
        self.returncode = returncode


class NodeShellTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_get_ydb_arg_sources_cfg_and_extracts_argument(self):
        service = self.make_service()
        calls = []

        async def shell(cmd):
            calls.append(cmd)
            return term.Result(0, "noise\n#727#727#--config cfg --grpc-port 2135\n", "")

        with mock.patch.object(nodes.term, "shell", shell):
            self.assertEqual(await service._get_ydb_arg("ydb_node_static_1"), "--config cfg --grpc-port 2135")

        self.assertEqual(calls, ['. /mnc/ydb_node_static_1/cfg/*.cfg && echo "#727#727#${ydb_arg}"'])

    async def test_get_ydb_arg_returns_none_when_shell_fails(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(1, "", "bad cfg")

        with mock.patch.object(nodes.term, "shell", shell):
            self.assertIsNone(await service._get_ydb_arg("node"))

    async def test_start_node_rejects_disabled_node_without_process_creation(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("node")
        node_info.enabled = False

        async def create_subprocess_exec(*args, **kwargs):
            raise AssertionError("process should not be created")

        with mock.patch.object(nodes.asyncio, "create_subprocess_exec", create_subprocess_exec):
            result = await service._start_node(node_info)

        self.assertFalse(result.success)
        self.assertEqual(result.message, "Node node is disabled")

    async def test_start_node_creates_process_and_writes_pid_file(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("node")
        shell_calls = []
        process_calls = []

        async def get_ydb_arg(node):
            self.assertEqual(node, "node")
            return "--config /mnc/node/cfg/config.yaml --grpc-port 2135"

        async def shell(cmd):
            shell_calls.append(cmd)
            if cmd == "pgrep -P 1000":
                return term.Result(0, "2000\n", "")
            return term.Result(0, "", "")

        async def create_subprocess_exec(*args, **kwargs):
            process_calls.append((args, kwargs))
            return FakeProcess(pid=1000)

        file_handle = mock.mock_open()
        with mock.patch.object(service, "_get_ydb_arg", get_ydb_arg), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes.asyncio, "create_subprocess_exec", create_subprocess_exec), \
                mock.patch.object(nodes.os, "makedirs") as makedirs, \
                mock.patch("builtins.open", file_handle):
            result = await service._start_node(node_info)

        self.assertTrue(result.success)
        makedirs.assert_called_once_with("/mnc/run", exist_ok=True)
        self.assertEqual(process_calls[0][0], (
            "sudo",
            "-E",
            "/mnc/ydb/bin/ydb",
            "--config",
            "/mnc/node/cfg/config.yaml",
            "--grpc-port",
            "2135",
        ))
        self.assertTrue(process_calls[0][1]["start_new_session"])
        self.assertEqual(shell_calls, [
            "sudo touch /mnc/node/stdout /mnc/node/stderr",
            "sudo chmod 666 /mnc/node/stdout /mnc/node/stderr",
            "pgrep -P 1000",
            "sudo bash -c 'echo 2000 > /mnc/run/node.pid'",
        ])
        self.assertEqual(node_info.pid, 2000)
        self.assertEqual(service.persistent_service.saved, [("node", 2000, True)])

    async def test_start_node_uses_parent_pid_when_child_pid_is_missing(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("node")

        async def get_ydb_arg(node):
            return "--config cfg"

        async def shell(cmd):
            if cmd == "pgrep -P 1000":
                return term.Result(1, "", "")
            return term.Result(0, "", "")

        async def create_subprocess_exec(*args, **kwargs):
            return FakeProcess(pid=1000)

        with mock.patch.object(service, "_get_ydb_arg", get_ydb_arg), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes.asyncio, "create_subprocess_exec", create_subprocess_exec), \
                mock.patch.object(nodes.os, "makedirs"), \
                mock.patch("builtins.open", mock.mock_open()):
            result = await service._start_node(node_info)

        self.assertTrue(result.success)
        self.assertEqual(node_info.pid, 1000)

    async def test_start_node_kills_process_when_pid_file_write_fails(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("node")
        shell_calls = []

        async def get_ydb_arg(node):
            return "--config cfg"

        async def shell(cmd):
            shell_calls.append(cmd)
            if cmd == "pgrep -P 1000":
                return term.Result(0, "2000\n", "")
            if cmd == "sudo bash -c 'echo 2000 > /mnc/run/node.pid'":
                return term.Result(1, "", "write failed")
            return term.Result(0, "", "")

        async def create_subprocess_exec(*args, **kwargs):
            return FakeProcess(pid=1000)

        with mock.patch.object(service, "_get_ydb_arg", get_ydb_arg), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes.asyncio, "create_subprocess_exec", create_subprocess_exec), \
                mock.patch.object(nodes.os, "makedirs"), \
                mock.patch("builtins.open", mock.mock_open()):
            result = await service._start_node(node_info)

        self.assertFalse(result.success)
        self.assertIn("Failed to write PID file", result.message)
        self.assertIn("sudo kill -TERM 2000", shell_calls)

    async def test_manual_stop_node_uses_local_kill_command(self):
        service = self.make_service()
        calls = []

        async def shell(cmd):
            calls.append(cmd)
            return term.Result(0, "", "")

        with mock.patch.object(nodes.term, "shell", shell):
            result = await service._manual_stop_node("node", pid=123)

        self.assertTrue(result.success)
        self.assertEqual(calls, ["sudo kill -9 123"])

    async def test_manual_stop_node_reports_kill_failure(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(1, "", "no process")

        with mock.patch.object(nodes.term, "shell", shell):
            result = await service._manual_stop_node("node", pid=123)

        self.assertFalse(result.success)
        self.assertIn("Failed to stop node", result.message)


class NodeStatusTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_get_nodes_lists_ydb_node_directories(self):
        service = self.make_service()

        async def shell(cmd):
            if cmd == "ls /mnc | grep ydb_node":
                return term.Result(0, "ydb_node_static_1\nydb_node_dynamic_1\n", "")
            return term.Result(0, "", "")

        async def fake_status(node):
            return mock.MagicMock(node=node, running=False, enabled=False)

        with mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(service, "_get_status", fake_status):
            response = await service.get_nodes()

        self.assertEqual([n.node for n in response.nodes], ["ydb_node_static_1", "ydb_node_dynamic_1"])
        self.assertIsNone(response.error)

    async def test_get_nodes_reports_listing_failure(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(1, "", "permission denied")

        with mock.patch.object(nodes.term, "shell", shell):
            response = await service.get_nodes()

        self.assertEqual(response.error, "Failed to get nodes")
        self.assertIn("permission denied", response.message)
        self.assertEqual(response.nodes, [])

    async def test_get_status_reports_in_memory_running_node(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("ydb_node_static_1")
        node_info.pid = 42
        node_info.enabled = True
        service._node_infos["ydb_node_static_1"] = node_info

        async def is_running(node, pid):
            return True

        with mock.patch.object(nodes, "is_running", is_running):
            status = await service._get_status("ydb_node_static_1")

        self.assertTrue(status.running)
        self.assertEqual(status.pid, 42)
        self.assertTrue(status.enabled)

    async def test_get_status_missing_pid_file(self):
        service = self.make_service()
        with mock.patch.object(nodes.os.path, "exists", lambda path: False):
            status = await service._get_status("ydb_node_static_1")
        self.assertFalse(status.running)
        self.assertEqual(status.error, "PID file not found")
        self.assertFalse(status.by_agent)

    async def test_get_status_empty_pid_file(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(0, "\n", "")

        with mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell):
            status = await service._get_status("ydb_node_static_1")
        self.assertFalse(status.running)
        self.assertEqual(status.error, "PID file is empty")

    async def test_get_status_dead_pid(self):
        service = self.make_service()
        shell_calls = []

        async def shell(cmd):
            shell_calls.append(cmd)
            if "cat " in cmd:
                return term.Result(0, "1234\n", "")
            return term.Result(0, "", "")

        async def is_running(node, pid):
            return False

        with mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes, "is_running", is_running):
            status = await service._get_status("ydb_node_static_1")

        self.assertFalse(status.running)
        self.assertEqual(status.pid, 1234)

    async def test_get_status_external_running_pid(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(0, "1234\n", "")

        async def is_running(node, pid):
            return pid == 1234

        with mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes, "is_running", is_running):
            status = await service._get_status("ydb_node_static_1")

        self.assertTrue(status.running)
        self.assertEqual(status.pid, 1234)
        self.assertFalse(status.by_agent)

    async def test_get_status_pid_read_failure(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(1, "", "read fail")

        with mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell):
            status = await service._get_status("ydb_node_static_1")
        self.assertFalse(status.running)
        self.assertEqual(status.error, "Failed to read PID file")

    async def test_get_status_invalid_pid_in_file(self):
        service = self.make_service()

        async def shell(cmd):
            return term.Result(0, "not_a_number\n", "")

        with mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell):
            status = await service._get_status("ydb_node_static_1")

        self.assertFalse(status.running)
        self.assertIsNotNone(status.error)
        self.assertIn("Failed to check", status.error)


class NodeStopNodesTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_stop_nodes_already_stopped_reports_success(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("n1")
        service._node_infos["n1"] = node_info

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[NodeStatusSchema(node="n1", running=False)])

        async def is_running(node, pid):
            return False

        with mock.patch.object(service, "get_nodes", get_nodes), \
                mock.patch.object(nodes, "is_running", is_running):
            response = await service.stop_nodes(["n1"], wait=False)

        self.assertTrue(response.operations[0].success)
        self.assertIn("already stopped", response.operations[0].message)

    async def test_stop_nodes_skips_not_installed_nodes(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("n1")
        node_info.pid = 42
        service._node_infos["n1"] = node_info

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[NodeStatusSchema(node="n1", running=True)])

        shell_calls = []

        async def shell(cmd):
            shell_calls.append(cmd)
            return term.Result(0, "", "")

        async def is_running(node, pid):
            return True

        async def get_pid(node):
            return 42

        with mock.patch.object(service, "get_nodes", get_nodes), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes, "get_pid", get_pid):
            response = await service.stop_nodes(["n1", "n2"], wait=False)

        by_node = {operation.node: operation for operation in response.operations}
        self.assertTrue(by_node["n1"].success)
        self.assertIn("Stopped node", by_node["n1"].message)
        self.assertTrue(by_node["n2"].success)
        self.assertIn("not installed", by_node["n2"].message)
        self.assertEqual([call for call in shell_calls if "kill" in call], ["sudo kill -9 42"])

    async def test_stop_nodes_wait_removes_pid_file_and_preserves_enabled(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("n1")
        node_info.pid = 42
        node_info.enabled = False
        service._node_infos["n1"] = node_info

        is_running_calls = []

        async def is_running(node, pid):
            is_running_calls.append((node, pid))
            return len(is_running_calls) == 1  # running first, then dead

        shell_calls = []

        async def shell(cmd):
            shell_calls.append(cmd)
            return term.Result(0, "", "")

        async def get_pid(node):
            return 42

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[NodeStatusSchema(node="n1", running=True)])

        with mock.patch.object(service, "get_nodes", get_nodes), \
                mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes, "get_pid", get_pid):
            response = await service.stop_nodes(["n1"], wait=True)

        self.assertTrue(response.operations[0].success)
        self.assertIn("sudo kill -9 42", shell_calls)
        self.assertIn("sudo rm -rf /mnc/run/n1.pid", shell_calls)
        # enabled value (False) is preserved when persisting after stop
        self.assertEqual(service.persistent_service.saved, [("n1", None, False)])

    async def test_stop_nodes_wait_timeout_marks_failure(self):
        service = self.make_service()
        node_info = nodes.NodeInfo("n1")
        node_info.pid = 42
        service._node_infos["n1"] = node_info

        async def is_running(node, pid):
            return True  # never dies

        async def shell(cmd):
            return term.Result(0, "", "")

        async def get_pid(node):
            return 42

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[NodeStatusSchema(node="n1", running=True)])

        with mock.patch.object(service, "get_nodes", get_nodes), \
                mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes.term, "shell", shell), \
                mock.patch.object(nodes, "get_pid", get_pid), \
                mock.patch.object(nodes.asyncio, "sleep", mock.AsyncMock(return_value=None)):
            response = await service.stop_nodes(["n1"], timeout=1, wait=True)

        self.assertFalse(response.operations[0].success)
        self.assertIn("still running", response.operations[0].message)


class NodeRestartTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_restart_returns_stop_result_when_stop_fails(self):
        service = self.make_service()

        async def stop_nodes(nodes_list, **kwargs):
            from ydb.tools.mnc.agent.schemas.node import (
                NodeServiceOperationBatchSchema,
                NodeServiceOperationSchema,
            )
            return NodeServiceOperationBatchSchema(
                operations=[
                    NodeServiceOperationSchema(node="n1", operation="stop_node", success=False, message="boom")
                ]
            )

        async def start_nodes(*args, **kwargs):
            raise AssertionError("start should not be called after failed stop")

        with mock.patch.object(service, "stop_nodes", stop_nodes), \
                mock.patch.object(service, "start_nodes", start_nodes):
            response = await service.restart_nodes(["n1"])

        self.assertFalse(response.operations[0].success)
        self.assertEqual(response.operations[0].message, "boom")

    async def test_restart_propagates_force_flag_to_start(self):
        service = self.make_service()
        captured = {}

        async def stop_nodes(nodes_list, **kwargs):
            from ydb.tools.mnc.agent.schemas.node import (
                NodeServiceOperationBatchSchema,
                NodeServiceOperationSchema,
            )
            return NodeServiceOperationBatchSchema(
                operations=[
                    NodeServiceOperationSchema(node="n1", operation="stop_node", success=True)
                ]
            )

        async def start_nodes(nodes_list, **kwargs):
            captured.update(kwargs)
            from ydb.tools.mnc.agent.schemas.node import (
                NodeServiceOperationBatchSchema,
                NodeServiceOperationSchema,
            )
            return NodeServiceOperationBatchSchema(
                operations=[
                    NodeServiceOperationSchema(node="n1", operation="start_node", success=True)
                ]
            )

        with mock.patch.object(service, "stop_nodes", stop_nodes), \
                mock.patch.object(service, "start_nodes", start_nodes):
            await service.restart_nodes(["n1"], force=True, wait=True, timeout=5)

        self.assertTrue(captured.get("force"))
        self.assertTrue(captured.get("wait"))
        self.assertEqual(captured.get("timeout"), 5)


class NodeEnableDisableTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_enable_node_when_already_enabled_returns_success_without_persist(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        info.enabled = True
        service._node_infos["n1"] = info

        result = await service.enable_node("n1")
        self.assertTrue(result.success)
        self.assertIn("already enabled", result.message)
        self.assertEqual(service.persistent_service.enabled_calls, [])

    async def test_enable_node_transitions_disabled_to_enabled(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        info.enabled = False
        service._node_infos["n1"] = info

        result = await service.enable_node("n1")
        self.assertTrue(result.success)
        self.assertTrue(info.enabled)
        self.assertEqual(service.persistent_service.enabled_calls, [("n1", True)])

    async def test_disable_node_when_already_disabled_returns_success(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        info.enabled = False
        service._node_infos["n1"] = info

        result = await service.disable_node("n1")
        self.assertTrue(result.success)
        self.assertIn("already disabled", result.message)
        self.assertEqual(service.persistent_service.enabled_calls, [])

    async def test_disable_node_transitions_enabled_to_disabled(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        info.enabled = True
        service._node_infos["n1"] = info

        result = await service.disable_node("n1")
        self.assertTrue(result.success)
        self.assertFalse(info.enabled)
        self.assertEqual(service.persistent_service.enabled_calls, [("n1", False)])


class NodeInstallTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_install_nodes_rejects_when_nodes_exist(self):
        service = self.make_service()
        from ydb.tools.mnc.agent.schemas.node import NodesResponseSchema, NodeStatusSchema

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[NodeStatusSchema(node="ydb_node_static_1", running=False)])

        with mock.patch.object(service, "get_nodes", get_nodes):
            response = await service.install_nodes(InstallNodesRequest(yaml_config="cfg"))

        self.assertEqual(response.error, "There are already installed nodes")

    async def test_install_nodes_creates_static_and_dynamic_configs(self):
        service = self.make_service()
        from ydb.tools.mnc.agent.schemas.node import NodesResponseSchema

        writes = {}
        made_dirs = []

        def write_text(self, content):
            writes[str(self)] = content

        async def get_nodes(**kwargs):
            return NodesResponseSchema(nodes=[])

        async def fake_get_node_info(node):
            info = service._node_infos.get(node) or nodes.NodeInfo(node)
            service._node_infos[node] = info
            return info

        with mock.patch.object(service, "get_nodes", get_nodes), \
                mock.patch.object(service, "_get_node_info", fake_get_node_info), \
                mock.patch.object(nodes.os, "makedirs", lambda path, exist_ok=False: made_dirs.append(path)), \
                mock.patch.object(nodes.Path, "write_text", write_text):
            request = InstallNodesRequest(
                yaml_config="yaml-payload",
                node_broker_port=2135,
                static_node_params=[StaticNodeParams(ic_port=19001, mon_port=8765, grpc_port=2135)],
                dynamic_node_params=[DynamicNodeParams(ic_port=19002, mon_port=8766, grpc_port=2136, tenant="/Root/db", pile_name="pile1")],
            )
            response = await service.install_nodes(request)

        self.assertEqual(response.nodes, ["ydb_node_static_1", "ydb_node_dynamic_1"])
        self.assertIn("/mnc/ydb_node_static_1/cfg/config.yaml", writes)
        self.assertEqual(writes["/mnc/ydb_node_static_1/cfg/config.yaml"], "yaml-payload")
        self.assertIn("/mnc/ydb_node_static_1/cfg/node.cfg", writes)
        self.assertIn("/mnc/ydb_node_dynamic_1/cfg/node.cfg", writes)
        # _make_base_node_config persists disabled node info.
        self.assertIn(("ydb_node_static_1", None, False), service.persistent_service.saved)
        self.assertIn(("ydb_node_dynamic_1", None, False), service.persistent_service.saved)


class NodeUninstallTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        config.mnc_home = "/mnc"

    def tearDown(self):
        config.mnc_home = self.old_mnc_home

    def make_service(self):
        service = object.__new__(nodes.NodeService)
        service._mutex = asyncio.Lock()
        service._node_infos = {}
        service.persistent_service = PersistentMock()
        service.logger = nodes.logger
        return service

    async def test_uninstall_skips_existing_dir_when_missing(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        service._node_infos["n1"] = info

        async def is_running(node, pid):
            return False

        async def shell(cmd):
            return term.Result(0, "", "")

        with mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes.os.path, "exists", lambda path: False), \
                mock.patch.object(nodes.term, "shell", shell):
            response = await service.uninstall_nodes(["n1"])

        self.assertTrue(response.operations[0].success)
        self.assertIn("not installed", response.operations[0].message)
        self.assertEqual(service.persistent_service.deleted, ["n1"])

    async def test_uninstall_handles_delete_failure(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        service._node_infos["n1"] = info

        async def is_running(node, pid):
            return False

        async def shell(cmd):
            if cmd.startswith("sudo rm -rf"):
                return term.Result(1, "", "permission denied")
            return term.Result(0, "", "")

        with mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell):
            response = await service.uninstall_nodes(["n1"])

        self.assertFalse(response.operations[0].success)
        self.assertIn("Failed to delete", response.operations[0].message)
        # Should not delete persistence for failed delete.
        self.assertEqual(service.persistent_service.deleted, [])

    async def test_uninstall_stop_false_skips_stop(self):
        service = self.make_service()
        info = nodes.NodeInfo("n1")
        info.pid = 42
        service._node_infos["n1"] = info

        is_running_calls = []

        async def is_running(node, pid):
            is_running_calls.append((node, pid))
            return True

        shell_calls = []

        async def shell(cmd):
            shell_calls.append(cmd)
            return term.Result(0, "", "")

        with mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(nodes.os.path, "exists", lambda path: True), \
                mock.patch.object(nodes.term, "shell", shell):
            response = await service.uninstall_nodes(["n1"], stop=False)

        self.assertTrue(response.operations[0].success)
        # No `kill` calls when stop=False
        self.assertFalse(any("kill" in c for c in shell_calls))

    async def test_uninstall_aborts_when_stop_fails(self):
        service = self.make_service()
        info_a = nodes.NodeInfo("n1")
        info_a.pid = 42
        info_b = nodes.NodeInfo("n2")
        info_b.pid = 43
        service._node_infos["n1"] = info_a
        service._node_infos["n2"] = info_b

        async def is_running(node, pid):
            return True

        async def stop_nodes(nodes_list, **kwargs):
            from ydb.tools.mnc.agent.schemas.node import (
                NodeServiceOperationBatchSchema,
                NodeServiceOperationSchema,
            )
            return NodeServiceOperationBatchSchema(operations=[
                NodeServiceOperationSchema(node="n1", operation="stop_node", success=False, message="boom"),
                NodeServiceOperationSchema(node="n2", operation="stop_node", success=False, message="boom"),
            ])

        async def shell(cmd):
            return term.Result(0, "", "")

        with mock.patch.object(nodes, "is_running", is_running), \
                mock.patch.object(service, "stop_nodes", stop_nodes), \
                mock.patch.object(nodes.term, "shell", shell):
            response = await service.uninstall_nodes(["n1", "n2"])

        self.assertFalse(response.operations[0].success)
        self.assertFalse(response.operations[1].success)
        # No persistence delete on aborted uninstall.
        self.assertEqual(service.persistent_service.deleted, [])
