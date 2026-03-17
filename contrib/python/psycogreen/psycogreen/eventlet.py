"""A wait callback to allow psycopg2 cooperation with eventlet.

Use `patch_psycopg()` to enable eventlet support in Psycopg.
"""

# Copyright (C) 2010-2020 Daniele Varrazzo <daniele.varrazzo@gmail.com>
# All rights reserved.  See COPYING file for details.


from __future__ import absolute_import

import psycopg2
from psycopg2 import extensions

from eventlet.hubs import trampoline


def patch_psycopg():
    """Configure Psycopg to be used with eventlet in non-blocking way."""
    if not hasattr(extensions, 'set_wait_callback'):
        raise ImportError(
            "support for coroutines not available in this Psycopg version (%s)"
            % psycopg2.__version__
        )

    extensions.set_wait_callback(eventlet_wait_callback)


def eventlet_wait_callback(conn, timeout=-1):
    """A wait callback useful to allow eventlet to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            trampoline(conn.fileno(), read=True)
        elif state == extensions.POLL_WRITE:
            trampoline(conn.fileno(), write=True)
        else:
            raise psycopg2.OperationalError("Bad result from poll: %r" % state)
