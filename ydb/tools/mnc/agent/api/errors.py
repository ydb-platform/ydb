import logging

from aiohttp import web


logger = logging.getLogger(__name__)


async def not_found_handler(request, exc=None):
    return web.json_response(
        {
            "error": "Not found",
            "message": "The requested resource was not found",
        },
        status=404,
    )


async def internal_error_handler(request, exc):
    logger.error("Internal server error: %s", exc)
    return web.json_response(
        {
            "error": "Internal server error",
            "message": "An unexpected error occurred",
        },
        status=500,
    )


@web.middleware
async def error_middleware(request, handler):
    try:
        return await handler(request)
    except web.HTTPNotFound as exc:
        return await not_found_handler(request, exc)
    except web.HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        return await internal_error_handler(request, exc)
