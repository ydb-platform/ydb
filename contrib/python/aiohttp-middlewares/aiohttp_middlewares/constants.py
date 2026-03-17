"""
=============================
aiohttp_middlewares.constants
=============================

Collection of constants for ``aiohttp_middlewares`` project.

"""

#: Tuple of idempotent HTTP methods
IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS", "TRACE")

#: Tuple of non-idempotent HTTP methods
NON_IDEMPOTENT_METHODS = ("DELETE", "PATCH", "POST", "PUT")
