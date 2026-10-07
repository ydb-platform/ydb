from dataclasses import asdict
from typing import Callable, Awaitable

from ydb.tools.mnc.agent.schemas.disk import DiskOperationRequest
from ydb.tools.mnc.agent.schemas.node import (
    InstallNodesRequest,
    NodeOperationRequest,
    NodeServiceOperationBatchSchema,
    UninstallNodesRequest,
)
from ydb.tools.mnc.agent.schemas.task import TaskResult
from ydb.tools.mnc.agent.services.disks import disk_service
from ydb.tools.mnc.agent.services.nodes import nodes_service
from ydb.tools.mnc.agent.services.tasks import TaskBasic


class OperationTask(TaskBasic):
    operation: str

    async def _run_operation(self):
        raise NotImplementedError

    async def do(self) -> TaskResult:
        result = await self._run_operation()
        data = asdict(result)
        return TaskResult(
            success=all(operation.get("success", False) for operation in data.get("operations", [])),
            message=f"{self.operation} completed",
            data=data,
        )


class NodeOperationTask(OperationTask):
    def __init__(self, operation: str, request: NodeOperationRequest):
        super().__init__()
        self.operation = operation
        self.request = request

    async def _run_operation(self):
        runners: dict[str, Callable[..., Awaitable]] = {
            "start": nodes_service.start_nodes,
            "stop": nodes_service.stop_nodes,
            "restart": nodes_service.restart_nodes,
        }
        runner = runners[self.operation]
        kwargs = {
            "timeout": self.request.timeout,
            "wait": self.request.wait,
        }
        if self.operation in {"start", "restart"}:
            kwargs["force"] = self.request.force
        return await runner(self.request.nodes, **kwargs)


class NodeEnableTask(OperationTask):
    def __init__(self, enabled: bool, request: NodeOperationRequest):
        super().__init__()
        self.operation = "enable" if enabled else "disable"
        self.enabled = enabled
        self.request = request

    async def _run_operation(self):
        operations = []
        for node in self.request.nodes:
            if self.enabled:
                operations.append(await nodes_service.enable_node(node))
            else:
                operations.append(await nodes_service.disable_node(node))
        return NodeServiceOperationBatchSchema(operations=operations)


class NodeUninstallTask(OperationTask):
    def __init__(self, request: UninstallNodesRequest):
        super().__init__()
        self.operation = "uninstall"
        self.request = request

    async def _run_operation(self):
        return await nodes_service.uninstall_nodes(
            self.request.nodes,
            timeout=self.request.timeout,
            stop=self.request.stop,
        )


class NodeInstallTask(TaskBasic):
    def __init__(self, request: InstallNodesRequest):
        super().__init__()
        self.request = request

    async def do(self) -> TaskResult:
        result = await nodes_service.install_nodes(self.request)
        data = asdict(result)
        return TaskResult(
            success=result.error is None,
            message=result.error or "install completed",
            data=data,
        )


class DiskOperationTask(OperationTask):
    def __init__(self, operation: str, request: DiskOperationRequest):
        super().__init__()
        self.operation = operation
        self.request = request

    async def _run_operation(self):
        runners = {
            "split": disk_service.split,
            "unite": disk_service.unite,
            "obliterate": disk_service.obliterate,
        }
        return await runners[self.operation](self.request)
