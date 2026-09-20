import logging
from dataclasses import asdict

from aiohttp import web

from ydb.tools.mnc.agent.schemas.node import InstallNodesRequest, NodeOperationRequest, UninstallNodesRequest
from ydb.tools.mnc.agent.services.features import FeatureStatus, features_service
from ydb.tools.mnc.agent.services.operations import NodeEnableTask, NodeInstallTask, NodeOperationTask, NodeUninstallTask
from ydb.tools.mnc.agent.services.nodes import nodes_service
from ydb.tools.mnc.agent.services.tasks import task_service


features_service.set_feature_status("nodes", FeatureStatus.ENABLED)

routes = web.RouteTableDef()
logger = logging.getLogger(__name__)


def _query_list(request, name: str):
    return request.query.getall(name, [])


async def _add_task(task):
    await task_service.add_task(task)
    return web.json_response({"task_id": task.task_id, "status": task.status.value})


@routes.get("/nodes")
async def get_nodes(request):
    try:
        return web.json_response(asdict(await nodes_service.get_nodes()))
    except Exception as exc:  # noqa: BLE001
        return web.json_response(
            {
                "error": "Failed to get nodes",
                "message": str(exc),
                "nodes": [],
            }
        )


@routes.get("/nodes/status")
async def get_batch_nodes_status(request):
    statuses = []
    for node_name in _query_list(request, "node"):
        logger.info("Get status for node %s", node_name)
        statuses.append(await nodes_service.status_node(node_name))
    return web.json_response({"nodes": [asdict(status) for status in statuses]})


@routes.post("/nodes/start")
async def start_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeOperationTask("start", NodeOperationRequest.from_dict(payload)))


@routes.post("/nodes/stop")
async def stop_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeOperationTask("stop", NodeOperationRequest.from_dict(payload)))


@routes.post("/nodes/restart")
async def restart_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeOperationTask("restart", NodeOperationRequest.from_dict(payload)))


@routes.post("/nodes/enable")
async def enable_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeEnableTask(True, NodeOperationRequest.from_dict(payload)))


@routes.post("/nodes/disable")
async def disable_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeEnableTask(False, NodeOperationRequest.from_dict(payload)))


@routes.post("/nodes/uninstall")
async def uninstall_batch_nodes(request):
    payload = await request.json()
    return await _add_task(NodeUninstallTask(UninstallNodesRequest.from_dict(payload)))


@routes.post("/nodes/install")
async def install_nodes(request):
    payload = await request.json()
    return await _add_task(NodeInstallTask(InstallNodesRequest.from_dict(payload)))
