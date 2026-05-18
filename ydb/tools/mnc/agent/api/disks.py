from dataclasses import asdict

from aiohttp import web

from ydb.tools.mnc.agent.schemas.disk import DiskCheckRequest, DiskInfoRequest, DiskOperationRequest
from ydb.tools.mnc.agent.services.disks import disk_service
from ydb.tools.mnc.agent.services.features import FeatureStatus, features_service


features_service.set_feature_status("disks", FeatureStatus.ENABLED)

routes = web.RouteTableDef()


@routes.post("/disks/check")
async def check_disks(request):
    payload = await request.json()
    return web.json_response(asdict(await disk_service.check(DiskCheckRequest.from_dict(payload))))


@routes.post("/disks/info")
async def get_disks_info(request):
    payload = await request.json()
    return web.json_response(asdict(await disk_service.info(DiskInfoRequest.from_dict(payload))))


@routes.post("/disks/split")
async def split_disks(request):
    payload = await request.json()
    return web.json_response(asdict(await disk_service.split(DiskOperationRequest.from_dict(payload))))


@routes.post("/disks/unite")
async def unite_disks(request):
    payload = await request.json()
    return web.json_response(asdict(await disk_service.unite(DiskOperationRequest.from_dict(payload))))


@routes.post("/disks/obliterate")
async def obliterate_disks(request):
    payload = await request.json()
    return web.json_response(asdict(await disk_service.obliterate(DiskOperationRequest.from_dict(payload))))
