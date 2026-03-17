# -*- coding: utf-8 -*-

import logging
import socket
from functools import lru_cache

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "internet_available_only",
    "log_info",
)

LOG_INFO = True
LOGGER = logging.getLogger(__name__)


def log_info(func):
    """Log some useful info."""
    if not LOG_INFO:
        return func

    def inner(self, *args, **kwargs):
        """Inner."""
        result = func(self, *args, **kwargs)

        LOGGER.debug("\n\n%s", func.__name__)
        LOGGER.debug("============================")
        if func.__doc__:
            LOGGER.debug('""" %s """', func.__doc__.strip())
        LOGGER.debug("----------------------------")
        if result is not None:
            LOGGER.debug(result)
        LOGGER.debug("\n++++++++++++++++++++++++++++")

        return result

    return inner


@lru_cache(maxsize=32)
def is_internet_available(host="8.8.8.8", port=53, timeout=3):
    """Check if internet is available.

    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error as ex:
        print(ex)
        return False


def internet_available_only(func):
    def inner(self, *args, **kwargs):
        """Inner."""
        if not is_internet_available():
            LOGGER.debug("\n\n%s", func.__name__)
            LOGGER.debug("============================")
            if func.__doc__:
                LOGGER.debug('""" %s """', func.__doc__.strip())
            LOGGER.debug("----------------------------")
            LOGGER.debug("Skipping because no Internet connection available.")
            LOGGER.debug("\n++++++++++++++++++++++++++++")
            return None

        result = func(self, *args, **kwargs)
        return result

    return inner
