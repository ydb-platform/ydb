import logging
from dataclasses import asdict

from aiohttp import web

from ydb.tools.mnc.agent.schemas.node import InstallNodesRequest
from ydb.tools.mnc.agent.services.features import FeatureStatus, features_service
from ydb.tools.mnc.agent.services.nodes import nodes_service


features_service.set_feature_status("nodes", FeatureStatus.ENABLED)

routes = web.RouteTableDef()
logger = logging.getLogger(__name__)


def _query_list(request, name: str):
    return request.query.getall(name, [])


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


@routes.get("/nodes/start")
async def start_batch_nodes(request):
    nodes = _query_list(request, "node")
    timeout = int(request.query.get("timeout", "30"))
    wait = request.query.get("wait", "true").lower() != "false"
    return web.json_response(asdict(await nodes_service.start_nodes(nodes, timeout=timeout, wait=wait)))


@routes.get("/nodes/stop")
async def stop_batch_nodes(request):
    nodes = _query_list(request, "node")
    timeout = int(request.query.get("timeout", "30"))
    wait = request.query.get("wait", "true").lower() != "false"
    return web.json_response(asdict(await nodes_service.stop_nodes(nodes, timeout=timeout, wait=wait)))


@routes.get("/nodes/restart")
async def restart_batch_nodes(request):
    nodes = _query_list(request, "node")
    timeout = int(request.query.get("timeout", "30"))
    wait = request.query.get("wait", "true").lower() != "false"
    return web.json_response(asdict(await nodes_service.restart_nodes(nodes, timeout=timeout, wait=wait)))


@routes.get("/nodes/enable")
async def enable_batch_nodes(request):
    results = []
    for node_name in _query_list(request, "node"):
        logger.info("Enable node %s", node_name)
        results.append(await nodes_service.enable_node(node_name))
    return web.json_response({"operations": [asdict(result) for result in results]})


@routes.get("/nodes/disable")
async def disable_batch_nodes(request):
    results = []
    for node_name in _query_list(request, "node"):
        logger.info("Disable node %s", node_name)
        results.append(await nodes_service.disable_node(node_name))
    return web.json_response({"operations": [asdict(result) for result in results]})


@routes.get("/nodes/uninstall")
async def uninstall_batch_nodes(request):
    nodes = _query_list(request, "node")
    timeout = int(request.query.get("timeout", "10"))
    do_not_stop = request.query.get("do_not_stop", "true").lower() != "false"
    return web.json_response(
        asdict(await nodes_service.uninstall_nodes(nodes, timeout=timeout, do_not_stop=do_not_stop))
    )


@routes.post("/nodes/install")
async def install_nodes(request):
    payload = await request.json()
    response = await nodes_service.install_nodes(InstallNodesRequest.from_dict(payload))
    return web.json_response(asdict(response))
