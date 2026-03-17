"""
==================
Timeout Middleware
==================

Middleware to ensure that request handling does not exceeds X seconds.

Usage
=====

.. code-block:: python

    from aiohttp import web
    from aiohttp_middlewares import (
        error_middleware,
        timeout_middleware,
    )

    # Basic usage
    app = web.Application(middlewares=[timeout_middleware(29.5)])

    # Ignore slow responses from list of urls
    slow_urls = (
        "/slow-url",
        "/very-slow-url",
        "/very/very/slow/url",
    )
    app = web.Application(
        middlewares=[timeout_middleware(4.5, ignore=slow_urls)]
    )

    # Ignore slow responsed from dict of urls. URL to ignore is a key,
    # value is a lone string with HTTP method or list of strings with
    # HTTP methods to ignore. HTTP methods are case-insensitive
    slow_urls = {
        "/slow-url": "POST",
        "/very-slow-url": ("GET", "POST"),
    }
    app = web.Application(
        middlewares=[timeout_middleware(4.5, ignore=slow_urls)]
    )

    # Handle timeout errors with error middleware
    app = web.Application(
        middlewares=[error_middleware(), timeout_middleware(14.5)]
    )

"""

import logging
from typing import Union

from aiohttp import web
from async_timeout import timeout

from aiohttp_middlewares.annotations import Handler, Middleware, Urls
from aiohttp_middlewares.utils import match_request


logger = logging.getLogger(__name__)


def timeout_middleware(
    seconds: Union[int, float], *, ignore: Union[Urls, None] = None
) -> Middleware:
    """Ensure that request handling does not exceed X seconds.

    This is helpful when aiohttp application served behind nginx or other
    reverse proxy with enabled read timeout. And when this read timeout exceeds
    reverse proxy generates error page instead of aiohttp app, which may result
    in bad user experience.

    For best results, please do not supply seconds value which equals read
    timeout value at reverse proxy as it may results that request handling at
    aiohttp will be ended after reverse proxy already responded with 504 error.
    Timeout context manager accepts floats, so if nginx has read timeout in
    30 seconds, it's ok to configure timeout middleware to raise timeout error
    after 29.5 seconds. In that case in most cases user for sure will see the
    error from aiohttp app instead of reverse proxy.

    Notice that timeout middleware just raised :class:`asyncio.TimeoutError` in
    case of exceeding seconds per request, but not handling the error by
    itself. If you need to handle this error, please place
    :func:`aiohttp_middlewares.error.error_middleware` in list of
    application middlewares as well. Error middleware should be placed before
    timeout middleware, so timeout errors can be catched and processed
    properly.

    In case if you need to "disable" timeout middleware for given request path,
    please supply ignore collection as second positional argument, like:

    .. code-block:: python

        from aiohttp import web

        app = web.Application(
            middlewares=[timeout_middleware(14.5, ignore={"/slow-url"})]
        )

    In case if you need more flexible ignore rules you can pass ``ignore``
    dict, where key is an URL to ignore and value is a collection of methods to
    ignore from timeout handling for given URL.

    .. code-block:: python

        ignore = {"/slow-url": ["POST"]}
        app = web.Application(
            middlewares=[timeout_middleware(14.5, ignore=ignore)]
        )

    Behind the scene, when current request path match the URL from ignore
    collection or dict timeout context manager will be configured to avoid
    break the execution after X seconds.

    :param seconds: Max amount of seconds for each handler call.
    :param ignore:
        Do not limit execution for any of given URLs (paths). This is useful
        when request handler returns ``StreamResponse`` instead of regular
        ``Response``. You also can specify URLs as dict, where key is URL to
        ignore from wrapping into timeout context and value is list of methods
        to ignore. This is helpful when you need ignore only POST requests of
        slow API endpoint, but still need to have GET requests to same endpoint
        to not exceed X seconds.
    """

    @web.middleware
    async def middleware(
        request: web.Request, handler: Handler
    ) -> web.StreamResponse:
        """Wrap request handler into timeout context manager."""
        request_method = request.method
        request_path = request.rel_url.path

        if ignore and match_request(ignore, request_method, request_path):
            logger.debug(
                "Ignore path from timeout handling",
                extra={"method": request_method, "path": request_path},
            )
            return await handler(request)

        async with timeout(seconds):
            return await handler(request)

    return middleware
