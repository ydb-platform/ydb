"""A wait callback to allow psycopg2 cooperation with gevent.

Use `patch_psycopg()` to enable gevent support in Psycopg.
"""

# Copyright (C) 2010-2020 Daniele Varrazzo <daniele.varrazzo@gmail.com>
# All rights reserved.  See COPYING file for details.


from __future__ import absolute_import

import psycopg2
from psycopg2 import extensions

from gevent.socket import wait_read, wait_write


def patch_psycopg():
    """Configure Psycopg to be used with gevent in non-blocking way."""
    if not hasattr(extensions, 'set_wait_callback'):
        raise ImportError(
            "support for coroutines not available in this Psycopg version (%s)"
            % psycopg2.__version__
        )

    extensions.set_wait_callback(gevent_wait_callback)


def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise psycopg2.OperationalError("Bad result from poll: %r" % state)
