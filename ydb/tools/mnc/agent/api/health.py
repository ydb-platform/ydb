from aiohttp import web

from ydb.tools.mnc.agent.services.features import FeatureStatus, features_service


features_service.set_feature_status("health", FeatureStatus.ENABLED)

routes = web.RouteTableDef()


@routes.get("/health")
async def health_check(request):
    return web.json_response(
        {
            "status": "healthy",
            "service": "MNCAgentServer",
            "enabled_features": features_service.get_enabled_features(),
        }
    )
