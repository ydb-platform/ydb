import unittest
from unittest import mock

from ydb.tools.mnc.cli.ut.helpers import ParentTask
from ydb.tools.mnc.lib import service


class ServiceAgentOnlyTest(unittest.IsolatedAsyncioTestCase):
    async def test_cmd_rejects_unknown_command_without_calling_backends(self):
        async def cmd_agent_ydb_operation(*args, **kwargs):
            raise AssertionError("agent backend should not be called")

        with mock.patch.object(service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertFalse(await service.cmd("remove", "host1", ["node1"], has_agent=True))

    async def test_cmd_fails_when_agent_is_unavailable(self):
        async def cmd_agent_ydb_operation(*args, **kwargs):
            raise AssertionError("agent backend should not be called")

        with mock.patch.object(service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertFalse(await service.cmd("start", "host1", ["node1"], has_agent=False))

    async def test_cmd_uses_agent_backend_when_agent_is_available(self):
        calls = []

        async def cmd_agent_ydb_operation(host, command, processes):
            calls.append((host, command, processes))
            return True

        with mock.patch.object(service, "cmd_agent_ydb_operation", cmd_agent_ydb_operation):
            self.assertTrue(await service.cmd("start", "host1", ["node1"], has_agent=True))

        self.assertEqual(calls, [("host1", "start", ["node1"])])

    async def test_cmd_agent_ydb_operation_posts_and_waits_for_task(self):
        calls = []

        async def post_json_and_wait(host, path, payload):
            calls.append((host, path, payload))
            return {
                "operations": [
                    {"success": True, "operation": "start_node", "node": "n1", "message": "ok"},
                ]
            }

        async def ssh_run(*args, **kwargs):
            raise AssertionError("ssh should not be used")

        with mock.patch.object(service.agent_client, "post_json_and_wait", post_json_and_wait), \
                mock.patch.object(service.term, "ssh_run", ssh_run):
            self.assertTrue(await service.cmd_agent_ydb_operation("host1", "start", ["n1"]))

        self.assertEqual(calls, [("host1", "/nodes/start", {"nodes": ["n1"], "wait": True})])

    async def test_cmd_agent_ydb_operation_uninstall_sends_stop_true(self):
        calls = []

        async def post_json_and_wait(host, path, payload):
            calls.append((host, path, payload))
            return {
                "operations": [
                    {"success": True, "operation": "uninstall_node", "node": "n1", "message": "ok"},
                ]
            }

        with mock.patch.object(service.agent_client, "post_json_and_wait", post_json_and_wait):
            self.assertTrue(await service.cmd_agent_ydb_operation("host1", "uninstall", ["n1"]))

        self.assertEqual(calls, [("host1", "/nodes/uninstall", {"nodes": ["n1"], "wait": True, "stop": True})])

    async def test_cmd_agent_ydb_operation_returns_false_on_failed_operation(self):
        async def post_json_and_wait(host, path, payload):
            return {
                "operations": [
                    {"success": False, "operation": "start_node", "node": "n1", "message": "bad"},
                ]
            }

        with mock.patch.object(service.agent_client, "post_json_and_wait", post_json_and_wait):
            self.assertFalse(await service.cmd_agent_ydb_operation("host1", "start", ["n1"]))

    async def test_one_host_fails_when_agent_is_unavailable(self):
        async def check_agent(host):
            return False

        async def ssh_run(*args, **kwargs):
            raise AssertionError("ssh should not be used")

        async def cmd(*args, **kwargs):
            raise AssertionError("cmd should not be called without agent")

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service.term, "ssh_run", ssh_run), \
                mock.patch.object(service, "cmd", cmd):
            self.assertFalse(await service.one_host("start", "host1", node_type="static", parent_task=ParentTask()))

    async def test_one_host_uses_agent_discovery_when_agent_is_available(self):
        calls = []

        async def check_agent(host):
            return True

        async def get_processes(host):
            calls.append(("get_processes", host))
            return ["ydb_node_static_1", "ydb_node_dynamic_2"]

        async def cmd(command, host, processes, force=False, has_agent=False):
            calls.append(("cmd", command, host, processes, force, has_agent))
            return True

        async def ssh_run(*args, **kwargs):
            raise AssertionError("ssh should not be used")

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service, "cmd", cmd), \
                mock.patch.object(service.term, "ssh_run", ssh_run):
            self.assertTrue(await service.one_host("stop", "host1", node_type="dynamic", parent_task=ParentTask()))

        self.assertEqual(calls, [
            ("get_processes", "host1"),
            ("cmd", "stop", "host1", ["ydb_node_dynamic_2"], False, True),
        ])

    async def test_get_dynamic_nodes_uses_agent_discovery(self):
        calls = []

        async def check_agent(host):
            calls.append(("check_agent", host))
            return True

        async def get_processes(host):
            calls.append(("get_processes", host))
            return ["ydb_node_static_1", "ydb_node_dynamic_2"]

        async def ssh_run(*args, **kwargs):
            raise AssertionError("ssh should not be used")

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service.term, "ssh_run", ssh_run):
            self.assertEqual(await service.get_dynamic_nodes_from_host("host1"), ["ydb_node_dynamic_2"])

        self.assertEqual(calls, [("check_agent", "host1"), ("get_processes", "host1")])

    async def test_get_dynamic_nodes_returns_empty_when_agent_is_unavailable(self):
        async def check_agent(host):
            return False

        async def get_processes(*args, **kwargs):
            raise AssertionError("processes should not be requested without agent")

        async def ssh_run(*args, **kwargs):
            raise AssertionError("ssh should not be used")

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service.term, "ssh_run", ssh_run):
            self.assertEqual(await service.get_dynamic_nodes_from_host("host1"), [])

    async def test_cmd_agent_ydb_operation_returns_false_when_result_is_none(self):
        async def post_json_and_wait(host, path, payload):
            return None

        with mock.patch.object(service.agent_client, "post_json_and_wait", post_json_and_wait):
            self.assertFalse(await service.cmd_agent_ydb_operation("host1", "start", ["n1"]))

    async def test_cmd_agent_ydb_operation_with_empty_operations_returns_true(self):
        async def post_json_and_wait(host, path, payload):
            return {"operations": []}

        with mock.patch.object(service.agent_client, "post_json_and_wait", post_json_and_wait):
            # all([]) is True; an empty operations list is treated as success.
            self.assertTrue(await service.cmd_agent_ydb_operation("host1", "start", []))

    async def test_one_host_batches_processes_in_groups_of_ten(self):
        cmd_calls = []

        async def check_agent(host):
            return True

        async def get_processes(host):
            return [f"ydb_node_static_{i}" for i in range(1, 13)]

        async def cmd(command, host, processes, force=False, has_agent=False):
            cmd_calls.append(list(processes))
            return True

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service, "cmd", cmd):
            self.assertTrue(await service.one_host(
                "start", "host1", node_type="static", parent_task=ParentTask()
            ))

        self.assertEqual(len(cmd_calls), 2)
        self.assertEqual(len(cmd_calls[0]), 10)
        self.assertEqual(len(cmd_calls[1]), 2)

    async def test_one_host_short_circuits_on_cmd_failure(self):
        cmd_calls = []

        async def check_agent(host):
            return True

        async def get_processes(host):
            return [f"ydb_node_static_{i}" for i in range(1, 25)]

        async def cmd(command, host, processes, force=False, has_agent=False):
            cmd_calls.append(list(processes))
            return False

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service, "cmd", cmd):
            self.assertFalse(await service.one_host(
                "start", "host1", node_type="static", parent_task=ParentTask()
            ))

        # Should abort after the first failing batch.
        self.assertEqual(len(cmd_calls), 1)

    async def test_one_host_explicit_node_ids_skip_discovery(self):
        get_processes_called = []

        async def check_agent(host):
            return True

        async def get_processes(host):
            get_processes_called.append(host)
            return []

        captured = []

        async def cmd(command, host, processes, force=False, has_agent=False):
            captured.append(list(processes))
            return True

        with mock.patch.object(service, "check_agent", check_agent), \
                mock.patch.object(service, "get_processes", get_processes), \
                mock.patch.object(service, "cmd", cmd):
            self.assertTrue(await service.one_host(
                "start", "host1", ydb_node_ids=[1, 3], node_type="static",
                parent_task=ParentTask(),
            ))

        # ydb_node_ids supplied → no auto-discovery.
        self.assertEqual(get_processes_called, [])
        self.assertEqual(captured, [["ydb_node_static_1", "ydb_node_static_3"]])

    async def test_act_hosts_aggregates_results_across_hosts(self):
        calls = []

        async def one_host(command, host, node_type=None, force=False, parent_task=None, subtasks=None):
            calls.append(host)
            if subtasks is not None:
                # mimic one_host appending its subtask so act_hosts can hide it
                from ydb.tools.mnc.cli.ut.helpers import Task as _Task
                subtasks.append(_Task())
            return host != "host2"

        with mock.patch.object(service, "one_host", one_host):
            result = await service.act_hosts(
                "start", ["host1", "host2", "host3"], node_type="static",
                parent_task=ParentTask(),
            )

        self.assertFalse(result)
        self.assertEqual(sorted(calls), ["host1", "host2", "host3"])

    async def test_act_hosts_returns_true_when_all_hosts_succeed(self):
        async def one_host(command, host, node_type=None, force=False, parent_task=None, subtasks=None):
            return True

        with mock.patch.object(service, "one_host", one_host):
            result = await service.act_hosts(
                "start", ["host1", "host2"], node_type="static",
                parent_task=ParentTask(),
            )

        self.assertTrue(result)
