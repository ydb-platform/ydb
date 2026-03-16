#
# Copyright (C) 2015 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# suppress warning about the name of 'getLogger':
# pylint: disable=invalid-name
"""anyconfig module initialization before any other things.
"""
import logging
import anyconfig.compat


def getLogger(name="anyconfig"):
    """
    See: "Configuring Logging for a Library" in python standard logging howto,
    e.g. https://docs.python.org/2/howto/logging.html#library-config.
    """
    logger = logging.getLogger(name)
    logger.addHandler(anyconfig.compat.NullHandler())

    return logger

# vim:sw=4:ts=4:et:
