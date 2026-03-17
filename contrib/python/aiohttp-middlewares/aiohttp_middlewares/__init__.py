"""
===========
Middlewares
===========

Collection of useful middlewares for aiohttp applications.

"""

from aiohttp_middlewares.constants import (
    IDEMPOTENT_METHODS,
    NON_IDEMPOTENT_METHODS,
)
from aiohttp_middlewares.cors import cors_middleware
from aiohttp_middlewares.error import (
    default_error_handler,
    error_context,
    error_middleware,
    get_error_response,
)
from aiohttp_middlewares.https import https_middleware
from aiohttp_middlewares.shield import shield_middleware
from aiohttp_middlewares.timeout import timeout_middleware
from aiohttp_middlewares.utils import match_path


__author__ = "Igor Davydenko"
__license__ = "BSD-3-Clause"
__version__ = "2.4.0"


# Make flake8 happy
(  # noqa: B018
    cors_middleware,
    default_error_handler,
    error_context,
    error_middleware,
    get_error_response,
    https_middleware,
    IDEMPOTENT_METHODS,
    match_path,
    NON_IDEMPOTENT_METHODS,
    shield_middleware,
    timeout_middleware,
)
