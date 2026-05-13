from fastapi import APIRouter

from ydb.tools.mnc.agent.schemas.disk import (
    DiskCheckRequest,
    DiskCheckResponse,
    DiskInfoRequest,
    DiskInfoResponse,
    DiskOperationRequest,
    DiskOperationResponse,
)
from ydb.tools.mnc.agent.services.disks import disk_service
from ydb.tools.mnc.agent.services.features import features_service, FeatureStatus


features_service.set_feature_status("disks", FeatureStatus.ENABLED)

router = APIRouter(prefix="/disks", tags=["disks"])


@router.post("/check", response_model=DiskCheckResponse)
async def check_disks(request: DiskCheckRequest):
    return await disk_service.check(request)


@router.post("/info", response_model=DiskInfoResponse)
async def get_disks_info(request: DiskInfoRequest):
    return await disk_service.info(request)


@router.post("/split", response_model=DiskOperationResponse)
async def split_disks(request: DiskOperationRequest):
    return await disk_service.split(request)


@router.post("/unite", response_model=DiskOperationResponse)
async def unite_disks(request: DiskOperationRequest):
    return await disk_service.unite(request)


@router.post("/obliterate", response_model=DiskOperationResponse)
async def obliterate_disks(request: DiskOperationRequest):
    return await disk_service.obliterate(request)
