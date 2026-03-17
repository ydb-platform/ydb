"""
================
HTTPS Middleware
================

Change scheme for current request when aiohttp application deployed behind
reverse proxy with HTTPS enabled.

Usage
=====

.. code-block:: python

    from aiohttp import web
    from aiohttp_middlewares import https_middleware

    # Basic usage
    app = web.Application(middlewares=[https_middleware()])

    # Specify custom headers to match, not `X-Forwarded-Proto: https`
    app = web.Application(
        middlewares=https_middleware({"Forwarded": "https"})
    )

"""

import logging
from typing import Union

from aiohttp import web

from aiohttp_middlewares.annotations import DictStrStr, Handler, Middleware


DEFAULT_MATCH_HEADERS = {"X-Forwarded-Proto": "https"}

logger = logging.getLogger(__name__)


def https_middleware(
    match_headers: Union[DictStrStr, None] = None
) -> Middleware:
    """
    Change scheme for current request when aiohttp application deployed behind
    reverse proxy with HTTPS enabled.

    This middleware is required to use, when your aiohttp app deployed behind
    nginx with HTTPS enabled, after aiohttp discounted
    ``secure_proxy_ssl_header`` keyword argument in
    https://github.com/aio-libs/aiohttp/pull/2299.

    :param match_headers:
        Dict of header(s) from reverse proxy to specify that aiohttp run behind
        HTTPS. By default:

        .. code-block:: python

            {"X-Forwarded-Proto": "https"}

    """

    @web.middleware
    async def middleware(
        request: web.Request, handler: Handler
    ) -> web.StreamResponse:
        """Change scheme of current request when HTTPS headers matched."""
        headers = DEFAULT_MATCH_HEADERS
        if match_headers is not None:
            headers = match_headers

        matched = any(
            request.headers.get(key) == value for key, value in headers.items()
        )

        if matched:
            logger.debug(
                "Substitute request URL scheme to https",
                extra={
                    "headers": headers,
                    "request_headers": dict(request.headers),
                },
            )
            request = request.clone(scheme="https")

        return await handler(request)

    return middleware
