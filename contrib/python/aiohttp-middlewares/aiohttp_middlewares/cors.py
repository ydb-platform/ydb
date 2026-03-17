r"""
===============
CORS Middleware
===============

.. versionadded:: 0.2.0

Dealing with CORS headers for aiohttp applications.

**IMPORTANT:** There is a `aiohttp-cors
<https://pypi.org/project/aiohttp_cors/>`_ library, which handles CORS
headers by attaching additional handlers to aiohttp application for
OPTIONS (preflight) requests. In same time this CORS middleware mimics the
logic of `django-cors-headers <https://pypi.org/project/django-cors-headers>`_,
where all handling done in the middleware without any additional handlers. This
approach allows aiohttp application to respond with CORS headers for OPTIONS or
wildcard handlers, which is not possible with ``aiohttp-cors`` due to
https://github.com/aio-libs/aiohttp-cors/issues/241 issue.

For detailed information about CORS (Cross Origin Resource Sharing) please
visit:

- `Wikipedia <https://en.m.wikipedia.org/wiki/Cross-origin_resource_sharing>`_
- Or `MDN <https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS>`_

.. versionchanged:: 2.1.0

If CORS request ends up with :class:`aiohttp.web.HTTPException` - the exception
will be used as a handler ``response`` and CORS headers will still be added to
it, which means that even ``aiohttp`` cannot properly handle request to some
URL, the response will still contain all necessary CORS headers.

.. note::

   Thanks `Kaarle Ritvanen <https://github.com/kunkku>`_ for fix in
   `aiohttp-middlewares#98
   <https://github.com/playpauseandstop/aiohttp-middlewares/pull/98>`_ and
   `Jonathan Heathcote <https://github.com/mossblaser>`_ for the review.

Configuration
=============

**IMPORTANT:** By default, CORS middleware do not allow any origins to access
content from your aiohttp appliction. Which means, you need carefully check
possible options and provide custom values for your needs.

Usage
=====

.. code-block:: python

    import re

    from aiohttp import web
    from aiohttp_middlewares import cors_middleware
    from aiohttp_middlewares.cors import DEFAULT_ALLOW_HEADERS

    # Unsecure configuration to allow all CORS requests
    app = web.Application(
        middlewares=[cors_middleware(allow_all=True)]
    )

    # Allow CORS requests from URL http://localhost:3000
    app = web.Application(
        middlewares=[
            cors_middleware(origins=["http://localhost:3000"])
        ]
    )

    # Allow CORS requests from all localhost urls
    app = web.Application(
        middlewares=[
            cors_middleware(
                origins=[re.compile(r"^https?\:\/\/localhost")]
            )
        ]
    )

    # Allow CORS requests from https://frontend.myapp.com as well
    # as allow credentials
    CORS_ALLOW_ORIGINS = ["https://frontend.myapp.com"]
    app = web.Application(
        middlewares=[
            cors_middleware(
                origins=CORS_ALLOW_ORIGINS,
                allow_credentials=True,
            )
        ]
    )

    # Allow CORS requests only for API urls
    app = web.Application(
        middelwares=[
            cors_middleware(
                origins=CORS_ALLOW_ORIGINS,
                urls=[re.compile(r"^\/api")],
            )
        ]
    )

    # Allow CORS requests for POST & PATCH methods, and for all
    # default headers and `X-Client-UID`
    app = web.Application(
        middlewares=[
            cors_middleware(
                origins=CORS_ALLOW_ORIGINS,
                allow_methods=("POST", "PATCH"),
                allow_headers=DEFAULT_ALLOW_HEADERS
                + ("X-Client-UID",),
            )
        ]
    )

"""

import logging
import re
from typing import Pattern, Tuple, Union

from aiohttp import web

from aiohttp_middlewares.annotations import (
    Handler,
    Middleware,
    StrCollection,
    UrlCollection,
)
from aiohttp_middlewares.utils import match_path


ACCESS_CONTROL = "Access-Control"
ACCESS_CONTROL_ALLOW = f"{ACCESS_CONTROL}-Allow"
ACCESS_CONTROL_ALLOW_CREDENTIALS = f"{ACCESS_CONTROL_ALLOW}-Credentials"
ACCESS_CONTROL_ALLOW_HEADERS = f"{ACCESS_CONTROL_ALLOW}-Headers"
ACCESS_CONTROL_ALLOW_METHODS = f"{ACCESS_CONTROL_ALLOW}-Methods"
ACCESS_CONTROL_ALLOW_ORIGIN = f"{ACCESS_CONTROL_ALLOW}-Origin"
ACCESS_CONTROL_EXPOSE_HEADERS = f"{ACCESS_CONTROL}-Expose-Headers"
ACCESS_CONTROL_MAX_AGE = f"{ACCESS_CONTROL}-Max-Age"
ACCESS_CONTROL_REQUEST_METHOD = f"{ACCESS_CONTROL}-Request-Method"

DEFAULT_ALLOW_HEADERS = (
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "dnt",
    "origin",
    "user-agent",
    "x-csrftoken",
    "x-requested-with",
)
DEFAULT_ALLOW_METHODS = ("DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT")
DEFAULT_URLS: Tuple[Pattern[str]] = (re.compile(r".*"),)

logger = logging.getLogger(__name__)


def cors_middleware(
    *,
    allow_all: bool = False,
    origins: Union[UrlCollection, None] = None,
    urls: Union[UrlCollection, None] = None,
    expose_headers: Union[StrCollection, None] = None,
    allow_headers: StrCollection = DEFAULT_ALLOW_HEADERS,
    allow_methods: StrCollection = DEFAULT_ALLOW_METHODS,
    allow_credentials: bool = False,
    max_age: Union[int, None] = None,
) -> Middleware:
    """Middleware to provide CORS headers for aiohttp applications.

    :param allow_all:
        When enabled, allow any Origin to access content from your aiohttp web
        application. **Please be careful with enabling this option as it may
        result in security issues for your application.** By default: ``False``
    :param origins:
        Allow content access for given list of origins. Support supplying
        strings for exact origin match or regex instances. By default: ``None``
    :param urls:
        Allow content access for given list of URLs in aiohttp application.
        By default: *apply CORS headers for all URLs*
    :param expose_headers:
        List of headers to be exposed with every CORS request. By default:
        ``None``
    :param allow_headers:
        List of allowed headers. By default:

        .. code-block:: python

            (
                "accept",
                "accept-encoding",
                "authorization",
                "content-type",
                "dnt",
                "origin",
                "user-agent",
                "x-csrftoken",
                "x-requested-with",
            )

    :param allow_methods:
        List of allowed methods. By default:

        .. code-block:: python

            ("DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT")

    :param allow_credentials:
        When enabled apply allow credentials header in response, which results
        in sharing cookies on shared resources. **Please be careful with
        allowing credentials for CORS requests.** By default: ``False``
    :param max_age: Access control max age in seconds. By default: ``None``
    """
    check_urls: UrlCollection = DEFAULT_URLS if urls is None else urls

    @web.middleware
    async def middleware(
        request: web.Request, handler: Handler
    ) -> web.StreamResponse:
        # Initial vars
        request_method = request.method
        request_path = request.rel_url.path

        # Is this an OPTIONS request
        is_options_request = request_method == "OPTIONS"

        # Is this a preflight request
        is_preflight_request = (
            is_options_request
            and ACCESS_CONTROL_REQUEST_METHOD in request.headers
        )

        # Log extra data
        log_extra = {
            "is_preflight_request": is_preflight_request,
            "method": request_method.lower(),
            "path": request_path,
        }

        # Check whether CORS should be enabled for given URL or not. By default
        # CORS enabled for all URLs
        if not match_items(check_urls, request_path):
            logger.debug(
                "Request should not be processed via CORS middleware",
                extra=log_extra,
            )
            return await handler(request)

        # If this is a preflight request - generate empty response
        if is_preflight_request:
            response = web.StreamResponse()
        # Otherwise - call actual handler
        else:
            try:
                response = await handler(request)
            # In case of ``HTTPException`` - use it as handler response
            except web.HTTPException as exc:
                response = web.Response(
                    headers=exc.headers,
                    status=exc.status,
                    reason=exc.reason,
                    text=exc.text,
                )

        # Now check origin heaer
        origin = request.headers.get("Origin")
        # Empty origin - do nothing
        if not origin:
            logger.debug(
                "Request does not have Origin header. CORS headers not "
                "available for given requests",
                extra=log_extra,
            )
            return response

        # Set allow credentials header if necessary
        if allow_credentials:
            response.headers[ACCESS_CONTROL_ALLOW_CREDENTIALS] = "true"

        # Check whether current origin satisfies CORS policy
        if not allow_all and not (origins and match_items(origins, origin)):
            logger.debug(
                "CORS headers not allowed for given Origin", extra=log_extra
            )
            return response

        # Now start supplying CORS headers
        # First one is Access-Control-Allow-Origin
        if allow_all and not allow_credentials:
            cors_origin = "*"
        else:
            cors_origin = origin
        response.headers[ACCESS_CONTROL_ALLOW_ORIGIN] = cors_origin

        # Then Access-Control-Expose-Headers
        if expose_headers:
            response.headers[ACCESS_CONTROL_EXPOSE_HEADERS] = ", ".join(
                expose_headers
            )

        # Now, if this is an options request, respond with extra Allow headers
        if is_options_request:
            response.headers[ACCESS_CONTROL_ALLOW_HEADERS] = ", ".join(
                allow_headers
            )
            response.headers[ACCESS_CONTROL_ALLOW_METHODS] = ", ".join(
                allow_methods
            )
            if max_age is not None:
                response.headers[ACCESS_CONTROL_MAX_AGE] = str(max_age)

        # If this is preflight request - do not allow other middlewares to
        # process this request
        if is_preflight_request:
            logger.debug(
                "Provide CORS headers with empty response for preflight "
                "request",
                extra=log_extra,
            )
            raise web.HTTPOk(text="", headers=response.headers)

        # Otherwise return normal response
        logger.debug("Provide CORS headers for request", extra=log_extra)
        return response

    return middleware


def match_items(items: UrlCollection, value: str) -> bool:
    """Go through all items and try to match item with given value."""
    return any(match_path(item, value) for item in items)
