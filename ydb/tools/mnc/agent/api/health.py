from fastapi import APIRouter
from ydb.tools.mnc.agent.services.features import features_service, FeatureStatus


features_service.set_feature_status("health", FeatureStatus.ENABLED)

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health_check():
    """Health check endpoint."""
    return {
        'status': 'healthy',
        'service': 'MNCAgentServer',
        'enabled_features': features_service.get_enabled_features()
    }
