from __future__ import absolute_import

"""
M2Crypto threading support, required for multithreaded applications.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

# M2Crypto
from M2Crypto import m2


def init() -> None:
    """
    Initialize threading support.
    """
    m2.threading_init()


def cleanup() -> None:
    """
    End and cleanup threading support.
    """
    m2.threading_cleanup()
