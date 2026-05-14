from fastapi import APIRouter, Query
from ydb.tools.mnc.agent.services.features import features_service, FeatureStatus
from ydb.tools.mnc.agent.services.nodes import nodes_service
from ydb.tools.mnc.agent.schemas.node import (
    NodesResponseSchema, NodeServiceOperationBatchSchema, InstallNodesRequest, InstallNodesResponse
)
import logging
from typing import List

features_service.set_feature_status("nodes", FeatureStatus.ENABLED)

router = APIRouter(prefix="/nodes", tags=["nodes"])

logger = logging.getLogger(__name__)


@router.get("", response_model=NodesResponseSchema)
async def get_nodes():
    try:
        return await nodes_service.get_nodes()
    except Exception as e:
        return NodesResponseSchema(
            error='Failed to get nodes',
            message=str(e),
            nodes=[]
        )


@router.get("/status", response_model=NodesResponseSchema)
async def get_batch_nodes_status(node: List[str] = Query(default=[], description="List of nodes to get status for")):
    statuses = []
    for node_name in node:
        logger.info(f"Get status for node {node_name}")
        statuses.append(await nodes_service.status_node(node_name))
    return NodesResponseSchema(
        nodes=statuses
    )


@router.get("/start", response_model=NodeServiceOperationBatchSchema)
async def start_batch_nodes(
    node: List[str] = Query(default=[], description="List of nodes to start"),
    timeout: int = Query(default=30, description="Timeout in seconds"),
    wait: bool = Query(default=True, description="Wait for nodes to start")
):
    return await nodes_service.start_nodes(node, timeout=timeout, wait=wait)


@router.get("/stop", response_model=NodeServiceOperationBatchSchema)
async def stop_batch_nodes(
    node: List[str] = Query(default=[], description="List of nodes to stop"),
    timeout: int = Query(default=30, description="Timeout in seconds"),
    wait: bool = Query(default=True, description="Wait for nodes to stop")
):
    return await nodes_service.stop_nodes(node, timeout=timeout, wait=wait)


@router.get("/restart", response_model=NodeServiceOperationBatchSchema)
async def restart_batch_nodes(
    node: List[str] = Query(default=[], description="List of nodes to restart"),
    timeout: int = Query(default=30, description="Timeout in seconds"),
    wait: bool = Query(default=True, description="Wait for nodes to restart")
):
    return await nodes_service.restart_nodes(node, timeout=timeout, wait=wait)


@router.get("/enable", response_model=NodeServiceOperationBatchSchema)
async def enable_batch_nodes(node: List[str] = Query(default=[], description="List of nodes to enable")):
    results = []
    for node_name in node:
        logger.info(f"Enable node {node_name}")
        results.append(await nodes_service.enable_node(node_name))
    return NodeServiceOperationBatchSchema(operations=results)


@router.get("/disable", response_model=NodeServiceOperationBatchSchema)
async def disable_batch_nodes(node: List[str] = Query(default=[], description="List of nodes to disable")):
    results = []
    for node_name in node:
        logger.info(f"Disable node {node_name}")
        results.append(await nodes_service.disable_node(node_name))
    return NodeServiceOperationBatchSchema(operations=results)


@router.get("/uninstall", response_model=NodeServiceOperationBatchSchema)
async def uninstall_batch_nodes(
    node: List[str] = Query(default=[], description="List of nodes to uninstall"),
    timeout: int = Query(default=10, description="Timeout in seconds"),
    do_not_stop: bool = Query(default=True, description="Do not stop nodes before uninstalling")
):
    return await nodes_service.uninstall_nodes(node, timeout=timeout, do_not_stop=do_not_stop)


@router.post("/install", response_model=InstallNodesResponse)
async def install_nodes(request: InstallNodesRequest):
    return await nodes_service.install_nodes(request)
