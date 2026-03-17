# -*- coding: utf-8 -*-
"""
momoko
======

Momoko wraps Psycopg2's functionality for use in Tornado.

Copyright 2011-2014, Frank Smit & Zaar Hai.
MIT, see LICENSE for more details.
"""

import psycopg2

from .connection import Pool, Connection, connect
from .exceptions import PoolError, PartiallyConnectedError


try:
    psycopg2.extensions.POLL_OK
except AttributeError:
    import warnings
    warnings.warn(RuntimeWarning(
        'Psycopg2 does not have support for asynchronous connections. '
        'You need at least version 2.2.0 of Psycopg2 to use Momoko.'))
