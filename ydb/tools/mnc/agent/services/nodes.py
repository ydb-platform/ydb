import asyncio
import asyncio.subprocess as subprocess
import logging
import os
import shlex
from functools import wraps
from pathlib import Path
from typing import List, Optional

import ydb.tools.mnc.lib.templates as templates
from ydb.tools.mnc.lib import term

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.schemas.node import (
    DynamicNodeParams,
    InstallNodesRequest,
    InstallNodesResponse,
    NodeServiceOperationBatchSchema,
    NodeServiceOperationSchema,
    NodeStatusSchema,
    NodesResponseSchema,
    StaticNodeParams,
)
from ydb.tools.mnc.agent.services.database import DatabaseService, database_service


logger = logging.getLogger(__name__)


async def is_running(node: str, pid: int):
    result = await term.shell(f"ps -p {pid} > /dev/null 2>&1")
    return result.returncode == 0


async def get_pid(node: str):
    pid_result = await term.shell(f"cat {config.mnc_home}/run/{node}.pid")
    if pid_result.returncode != 0:
        logger.info("Failed to get PID for node %s: %s", node, pid_result.stderr)
        return None
    pid = pid_result.stdout.strip()
    if not pid:
        logger.info("PID file for node %s is empty", node)
        return None
    return int(pid)


class NodeInfo:
    def __init__(self, node: str):
        self._mutex = asyncio.Lock()
        self.node = node
        self.proc = None
        self.pid = None
        self.enabled = True

    async def set_proc(self, proc: subprocess.Process):
        if self.proc and not self.proc.returncode:
            await term.shell(f"sudo kill -TERM {self.proc.pid}")
        self.proc = proc

        child_pid = None
        for _ in range(10):
            res = await term.shell(f"pgrep -P {proc.pid}")
            if res.returncode == 0 and res.stdout.strip():
                child_pid = int(res.stdout.strip().split()[0])
                break
            await asyncio.sleep(0.1)

        if child_pid is None:
            logger.warning("Failed to get child PID for node %s", self.node)

        self.pid = child_pid or proc.pid

    async def set_external_pid(self, pid: int):
        self.pid = pid
        if self.proc and not self.proc.returncode:
            await term.shell(f"sudo kill -TERM {self.proc.pid}")
        self.proc = None

    async def is_running(self) -> bool:
        if self.proc is None and self.pid is not None:
            return await is_running(self.node, self.pid)
        return self.proc is not None and self.proc.returncode is None and await is_running(self.node, self.proc.pid)

    def by_agent(self) -> bool:
        return self.proc is not None


class NodeServicePersistent:
    def __init__(self, database_service: DatabaseService):
        self.database_service = database_service
        self.logger = logging.getLogger(__name__)

    async def init_tables(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS node_info (
            node TEXT PRIMARY KEY,
            pid INTEGER,
            enabled BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        await self.database_service.execute(create_table_sql)
        await self.database_service.commit()
        self.logger.info("Node info tables initialized")
        await self._run_migrations()

    async def _run_migrations(self):
        try:
            check_column_sql = """
            SELECT COUNT(*) FROM pragma_table_info('node_info')
            WHERE name = 'enabled'
            """
            result = await self.database_service.fetch_one(check_column_sql)
            if result and result[0] == 0:
                add_column_sql = """
                ALTER TABLE node_info ADD COLUMN enabled BOOLEAN DEFAULT TRUE
                """
                await self.database_service.execute(add_column_sql)
                await self.database_service.commit()
                self.logger.info("Added 'enabled' column to node_info table")
        except Exception as exc:
            self.logger.error("Migration failed: %s", exc)
            raise

    async def save_node_info(self, node: str, pid: Optional[int], enabled: bool = True):
        upsert_sql = """
        INSERT OR REPLACE INTO node_info (node, pid, enabled, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """
        await self.database_service.execute(upsert_sql, (node, pid, enabled))
        await self.database_service.commit()
        self.logger.info("Saved node info: %s, pid=%s, enabled=%s", node, pid, enabled)

    async def _make_node_info(self, node: str, pid: Optional[int], enabled: bool = True) -> NodeInfo:
        node_info = NodeInfo(node)
        if pid:
            await node_info.set_external_pid(pid)
        node_info.enabled = enabled
        return node_info

    async def load_node_info(self, node: str) -> Optional[NodeInfo]:
        select_sql = "SELECT node, pid, enabled FROM node_info WHERE node = ?"
        result = await self.database_service.fetch_one(select_sql, (node,))
        if result:
            node_name, pid, enabled = result
            return await self._make_node_info(node_name, pid, enabled)
        return None

    async def load_all_node_info(self) -> list:
        select_sql = "SELECT node, pid, enabled FROM node_info"
        raw_nodes = await self.database_service.fetch_all(select_sql)
        return [await self._make_node_info(node, pid, enabled) for node, pid, enabled in raw_nodes]

    async def set_node_enabled(self, node: str, enabled: bool):
        update_sql = """
        UPDATE node_info SET enabled = ?, updated_at = CURRENT_TIMESTAMP
        WHERE node = ?
        """
        await self.database_service.execute(update_sql, (enabled, node))
        await self.database_service.commit()
        self.logger.info("Set node %s enabled=%s", node, enabled)

    async def get_node_enabled(self, node: str) -> bool:
        select_sql = "SELECT enabled FROM node_info WHERE node = ?"
        result = await self.database_service.fetch_one(select_sql, (node,))
        return result[0] if result else True

    async def delete_node_info(self, node: str):
        delete_sql = "DELETE FROM node_info WHERE node = ?"
        await self.database_service.execute(delete_sql, (node,))
        await self.database_service.commit()
        self.logger.info("Deleted node info: %s", node)

    async def clear_all_node_info(self):
        delete_sql = "DELETE FROM node_info"
        await self.database_service.execute(delete_sql)
        await self.database_service.commit()
        self.logger.info("Cleared all node info")


def use_mutex(func):
    @wraps(func)
    async def wrapper(self, *args, use_mutex: bool = True, **kwargs):
        if use_mutex:
            async with self._mutex:
                return await func(self, *args, **kwargs)
        return await func(self, *args, **kwargs)

    return wrapper


class NodesRunningState:
    def __init__(self, running_nodes, not_running_nodes):
        self.running_nodes = running_nodes
        self.not_running_nodes = not_running_nodes


async def check_running_nodes(node_infos: List[NodeInfo]) -> NodesRunningState:
    running_nodes = []
    not_running_nodes = []
    for node_info in node_infos:
        async with node_info._mutex:
            if await node_info.is_running():
                running_nodes.append(node_info.node)
            else:
                not_running_nodes.append(node_info.node)
    return NodesRunningState(running_nodes, not_running_nodes)


class NodeService:
    def __init__(self):
        self._mutex = asyncio.Lock()
        self._node_infos = {}
        self.persistent_service = NodeServicePersistent(database_service)
        self.logger = logging.getLogger(__name__)
        database_service.add_init_task(self.init_node_info())

    @use_mutex
    async def init_node_info(self):
        await self.persistent_service.init_tables()

        saved_nodes = await self.persistent_service.load_all_node_info()
        for node_info in saved_nodes:
            self._node_infos[node_info.node] = node_info
            self.logger.info("Restored node info: %s, pid=%s", node_info.node, node_info.pid)

        current_nodes = await self.get_nodes(use_mutex=False)
        for node in current_nodes.nodes:
            current_info = self._node_infos.get(node.node)
            if current_info is None:
                continue
            async with current_info._mutex:
                if await current_info.is_running():
                    continue
                if node.pid and await is_running(node.node, node.pid):
                    await current_info.set_external_pid(node.pid)

    @use_mutex
    async def get_nodes(self) -> NodesResponseSchema:
        result = await term.shell(f"ls {config.mnc_home} | grep ydb_node")
        if result.returncode != 0:
            return NodesResponseSchema(
                error="Failed to get nodes",
                message=result.stderr.strip(),
                nodes=[],
            )
        nodes = result.stdout.split()
        return NodesResponseSchema(nodes=[await self._get_status(node) for node in nodes if node])

    async def _get_status(self, node: str) -> NodeStatusSchema:
        node_info = None
        enabled = False
        if node in self._node_infos:
            node_info = self._node_infos[node]
            if node_info is not None:
                async with node_info._mutex:
                    enabled = node_info.enabled
                    if await node_info.is_running():
                        return NodeStatusSchema(
                            node=node,
                            running=True,
                            by_agent=node_info.by_agent(),
                            pid=node_info.pid,
                            enabled=enabled,
                        )

        pid_file = f"{config.mnc_home}/run/{node}.pid"
        if not os.path.exists(pid_file):
            return NodeStatusSchema(
                node=node,
                running=False,
                by_agent=node_info is not None,
                error="PID file not found",
                pid_file=pid_file,
                enabled=enabled,
            )

        try:
            pid_result = await term.shell(f"cat '{pid_file}'")
            if pid_result.returncode != 0:
                return NodeStatusSchema(
                    node=node,
                    running=False,
                    by_agent=node_info is not None,
                    error="Failed to read PID file",
                    pid_file=pid_file,
                    enabled=enabled,
                )
            pid = pid_result.stdout.strip()

            if not pid:
                return NodeStatusSchema(
                    node=node,
                    running=False,
                    by_agent=node_info is not None,
                    error="PID file is empty",
                    pid_file=pid_file,
                )

            if await is_running(node, int(pid)):
                return NodeStatusSchema(
                    node=node,
                    running=True,
                    by_agent=False,
                    pid=int(pid),
                    pid_file=pid_file,
                    enabled=enabled,
                )

            return NodeStatusSchema(
                node=node,
                running=False,
                by_agent=node_info is not None,
                pid=int(pid),
                pid_file=pid_file,
                enabled=enabled,
            )
        except Exception as exc:
            return NodeStatusSchema(
                node=node,
                running=False,
                by_agent=False,
                error=f"Failed to check node status: {exc}",
                pid_file=pid_file,
                enabled=enabled,
            )

    async def _get_node_info(self, node: str) -> NodeInfo:
        if node in self._node_infos:
            return self._node_infos[node]
        status = await self._get_status(node)
        node_info = NodeInfo(node)
        if status.running and status.pid is not None:
            await node_info.set_external_pid(status.pid)
        self._node_infos[node] = node_info
        return node_info

    async def _manual_stop_node(self, node: str, pid=None) -> NodeServiceOperationSchema:
        pid = pid or await get_pid(node)
        if not pid:
            return NodeServiceOperationSchema(
                node=node,
                operation="stop_external_process",
                success=False,
                message=f"Failed to get PID for node {node}",
                data=None,
            )

        kill_result = await term.shell(f"sudo kill -9 {pid}")
        if kill_result.returncode != 0:
            return NodeServiceOperationSchema(
                node=node,
                operation="stop_external_process",
                success=False,
                message=f"Failed to stop node {node}; {kill_result.stderr}",
                data={"pid": pid, "mnc_home": config.mnc_home},
            )

        return NodeServiceOperationSchema(
            node=node,
            operation="stop_external_process",
            success=True,
            message=f"Stopped node {node}",
            data={"pid": pid, "mnc_home": config.mnc_home},
        )

    @use_mutex
    async def stop_nodes(self, nodes: List[str], timeout: int = 10, wait: bool = False) -> NodeServiceOperationBatchSchema:
        node_infos = [await self._get_node_info(node) for node in nodes]
        node_info_map = {node.node: node for node in node_infos}

        running_state = await check_running_nodes(node_infos)
        result = {
            node: NodeServiceOperationSchema(
                node=node,
                operation="stop_node",
                success=True,
                message=f"Node {node} is already stopped",
                data=None,
            )
            for node in running_state.not_running_nodes
        }
        for node in running_state.running_nodes:
            node_info = node_info_map[node]
            async with node_info._mutex:
                result[node] = await self._manual_stop_node(node)

        if wait:
            for _ in range(2 * timeout):
                running_state = await check_running_nodes(node_infos)
                if len(running_state.running_nodes) == 0:
                    break
                await asyncio.sleep(0.5)

            for node in running_state.running_nodes:
                if not result[node].success:
                    continue
                result[node].message += "; After stop, node is still running"
                result[node].success = False

            for node in running_state.not_running_nodes:
                await term.shell(f"sudo rm -rf {config.mnc_home}/run/{node}.pid")
                async with node_info_map[node]._mutex:
                    await self.persistent_service.save_node_info(node, None, False)

        return NodeServiceOperationBatchSchema(operations=[result[node] for node in nodes])

    async def _get_ydb_arg(self, node: str) -> Optional[str]:
        file_mask = f"{config.mnc_home}/{node}/cfg/*.cfg"
        separator = "#727#727#"
        echo_result = await term.shell(f'. {file_mask} && echo "{separator}${{ydb_arg}}"')
        if echo_result.returncode != 0:
            return None
        return echo_result.stdout.strip().split(separator)[1].strip()

    async def _start_node(self, node_info: NodeInfo) -> NodeServiceOperationSchema:
        async with node_info._mutex:
            node = node_info.node
            ydb_arg = await self._get_ydb_arg(node)
            if ydb_arg is None:
                return NodeServiceOperationSchema(
                    node=node,
                    operation="start_node",
                    success=False,
                    message=f"Failed to get ydb_arg for node {node}",
                    data=None,
                )
            bin_path = f"{config.mnc_home}/ydb/bin/ydb"
            stdout_path = f"{config.mnc_home}/{node}/stdout"
            stderr_path = f"{config.mnc_home}/{node}/stderr"
            await term.shell(f"sudo touch {stdout_path} {stderr_path}")
            await term.shell(f"sudo chmod 666 {stdout_path} {stderr_path}")
            out_f = open(stdout_path, "ab", buffering=0)
            err_f = open(stderr_path, "ab", buffering=0)

            cmd = ["sudo", "-E", bin_path, *shlex.split(ydb_arg)]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=out_f,
                stderr=err_f,
                start_new_session=True,
            )
            out_f.close()
            err_f.close()

            await node_info.set_proc(proc)
            pid_result = await term.shell(f"sudo bash -c 'echo {node_info.pid} > {config.mnc_home}/run/{node}.pid'")
            if not pid_result:
                await term.shell(f"sudo kill -TERM {node_info.pid}")
                return NodeServiceOperationSchema(
                    node=node,
                    operation="start_node",
                    success=False,
                    message=f"Failed to write PID file for node {node}: {pid_result.stderr}",
                    data={"pid": node_info.pid, "mnc_home": config.mnc_home},
                )
            await self.persistent_service.save_node_info(node, node_info.pid, True)

            return NodeServiceOperationSchema(
                node=node,
                operation="start_node",
                success=True,
                message=f"Started node {node}",
                data=None,
            )

    @use_mutex
    async def start_nodes(self, nodes: List[str], timeout: int = 10, wait: bool = False) -> NodeServiceOperationBatchSchema:
        node_infos = [await self._get_node_info(node) for node in nodes]
        node_info_map = {node.node: node for node in node_infos}

        running_state = await check_running_nodes(node_infos)
        result = {
            node: NodeServiceOperationSchema(
                node=node,
                operation="start_node",
                success=True,
                message=f"Node {node} is already running",
            )
            for node in running_state.running_nodes
        }

        for node in running_state.not_running_nodes:
            node_info = node_info_map[node]
            result[node] = await self._start_node(node_info)

        if wait:
            for _ in range(2 * timeout):
                running_state = await check_running_nodes(node_infos)
                if len(running_state.not_running_nodes) == 0:
                    break
                await asyncio.sleep(0.5)

            for node in running_state.not_running_nodes:
                if not result[node].success:
                    continue
                result[node].message += "; After start, node is not running"
                result[node].success = False

            for node in running_state.running_nodes:
                async with node_info_map[node]._mutex:
                    await self.persistent_service.save_node_info(
                        node, node_info_map[node].pid, node_info_map[node].enabled
                    )

        return NodeServiceOperationBatchSchema(operations=[result[node] for node in nodes])

    @use_mutex
    async def restart_nodes(self, nodes: List[str], timeout: int = 10, wait: bool = False) -> NodeServiceOperationBatchSchema:
        stop_result = await self.stop_nodes(nodes, timeout=timeout, wait=True, use_mutex=False)
        if not all(operation.success for operation in stop_result.operations):
            return stop_result
        return await self.start_nodes(nodes, timeout=timeout, wait=wait, use_mutex=False)

    @use_mutex
    async def enable_node(self, node: str) -> NodeServiceOperationSchema:
        node_info = await self._get_node_info(node)
        async with node_info._mutex:
            if node_info.enabled:
                return NodeServiceOperationSchema(
                    node=node,
                    operation="enable_node",
                    success=True,
                    message=f"Node {node} is already enabled",
                    data=None,
                )
            node_info.enabled = True
            await self.persistent_service.set_node_enabled(node, True)
            return NodeServiceOperationSchema(
                node=node,
                operation="enable_node",
                success=True,
                message=f"Node {node} enabled",
                data=None,
            )

    @use_mutex
    async def disable_node(self, node: str) -> NodeServiceOperationSchema:
        node_info = await self._get_node_info(node)
        async with node_info._mutex:
            if not node_info.enabled:
                return NodeServiceOperationSchema(
                    node=node,
                    operation="disable_node",
                    success=True,
                    message=f"Node {node} is already disabled",
                    data=None,
                )
            node_info.enabled = False
            await self.persistent_service.set_node_enabled(node, False)
            return NodeServiceOperationSchema(
                node=node,
                operation="disable_node",
                success=True,
                message=f"Node {node} disabled",
                data=None,
            )

    @use_mutex
    async def status_node(self, node: str) -> NodeStatusSchema:
        return await self._get_status(node)

    @use_mutex
    async def uninstall_nodes(self, nodes: List[str], timeout: int = 10, do_not_stop: bool = True) -> NodeServiceOperationBatchSchema:
        node_infos = [await self._get_node_info(node) for node in nodes]
        node_info_map = {node.node: node for node in node_infos}
        running_state = await check_running_nodes(node_infos)

        result = {}

        if running_state.running_nodes and not do_not_stop:
            stop_result = await self.stop_nodes(running_state.running_nodes, timeout=timeout, wait=True, use_mutex=False)
            ok = all(operation.success for operation in stop_result.operations)
            if not ok:
                result = {
                    node: NodeServiceOperationSchema(
                        node=node,
                        operation="uninstall_node",
                        success=False,
                        message=f"Node {node} is skipped",
                        data=None,
                    )
                    for node in running_state.not_running_nodes
                }
                for operation in stop_result.operations:
                    result[operation.node] = operation
                return NodeServiceOperationBatchSchema(operations=[result[node] for node in nodes])

        for node_info in node_infos:
            async with node_info._mutex:
                path = f"{config.mnc_home}/{node_info.node}"
                if not os.path.exists(path):
                    result[node_info.node] = NodeServiceOperationSchema(
                        node=node_info.node,
                        operation="uninstall_node",
                        success=True,
                        message=f"Node {node_info.node} is not installed",
                        data=None,
                    )
                    continue

                delete_result = await term.shell(f"sudo rm -rf {path}")
                if delete_result.returncode != 0:
                    result[node_info.node] = NodeServiceOperationSchema(
                        node=node_info.node,
                        operation="uninstall_node",
                        success=False,
                        message=f"Failed to delete node {node_info.node} directory",
                        data=None,
                    )
                    continue

                result[node_info.node] = NodeServiceOperationSchema(
                    node=node_info.node,
                    operation="uninstall_node",
                    success=True,
                    message=f"Node {node_info.node} uninstalled",
                    data=None,
                )

        for node in nodes:
            if not result[node].success:
                continue
            node_info = node_info_map[node]
            async with node_info._mutex:
                await self.persistent_service.delete_node_info(node)

        return NodeServiceOperationBatchSchema(operations=[result[node] for node in nodes])

    async def _make_base_node_config(self, node: str, yaml_config: str) -> str:
        node_path = f"{config.mnc_home}/{node}"
        os.makedirs(node_path, exist_ok=True)
        cfg_path = f"{node_path}/cfg"
        os.makedirs(cfg_path, exist_ok=True)
        log_path = f"{node_path}/logs"
        os.makedirs(log_path, exist_ok=True)

        cfg_file = f"{cfg_path}/config.yaml"
        Path(cfg_file).write_text(yaml_config)

        node_info = await self._get_node_info(node)
        async with node_info._mutex:
            await self.persistent_service.save_node_info(node, None, False)

        return node_info.node

    async def _make_static_node_config(self, node: str, yaml_config: str, params: StaticNodeParams) -> str:
        await self._make_base_node_config(node, yaml_config)
        ydb_config = templates.ydb_format.format(
            grpc_port=params.grpc_port,
            grpcs_port="",
            ic_port=params.ic_port,
            mon_port=params.mon_port,
            deploy_path=config.mnc_home,
            process_name=node,
            ca="",
            cert="",
            key="",
            mon_cert="",
        )
        cfg_path = f"{config.mnc_home}/{node}/cfg"
        Path(f"{cfg_path}/node.cfg").write_text(ydb_config)
        return node

    async def _make_dynamic_node_config(
        self,
        node: str,
        yaml_config: str,
        node_broker_port: int,
        params: DynamicNodeParams,
    ) -> str:
        node_info = await self._make_base_node_config(node, yaml_config)
        ydb_config = templates.dynamic_server_format.format(
            grpc_port=params.grpc_port,
            grpcs_port="",
            ic_port=params.ic_port,
            mon_port=params.mon_port,
            deploy_path=config.mnc_home,
            process_name=node,
            tenant=params.tenant,
            pile_name=params.pile_name or "",
            node_broker_port=node_broker_port,
            ca="",
            cert="",
            key="",
            mon_cert="",
        )
        cfg_path = f"{config.mnc_home}/{node}/cfg"
        Path(f"{cfg_path}/node.cfg").write_text(ydb_config)
        return node_info

    @use_mutex
    async def install_nodes(self, request: InstallNodesRequest) -> InstallNodesResponse:
        nodes = await self.get_nodes(use_mutex=False)
        if len(nodes.nodes) > 0:
            return InstallNodesResponse(error="There are already installed nodes")

        static_params = request.static_node_params or []
        dynamic_params = request.dynamic_node_params or []
        for idx, node in enumerate(static_params, start=1):
            await self._make_static_node_config(f"ydb_node_static_{idx}", request.yaml_config, node)
        for idx, node in enumerate(dynamic_params, start=1):
            await self._make_dynamic_node_config(
                f"ydb_node_dynamic_{idx}",
                request.yaml_config,
                request.node_broker_port,
                node,
            )

        return InstallNodesResponse(
            nodes=[
                f"ydb_node_static_{idx}" for idx in range(1, len(static_params) + 1)
            ]
            + [f"ydb_node_dynamic_{idx}" for idx in range(1, len(dynamic_params) + 1)]
        )


nodes_service = NodeService()
