"""Intercept HTTP connections that use
`urllib3 <https://urllib3.readthedocs.org/>`_.

Note that currently only urllib3 <2.0.0 is supported. 2.0.0 support
is in progress.
"""

from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.connection import HTTPConnection, HTTPSConnection
from ._urllib3 import make_urllib3_override


install, uninstall = make_urllib3_override(HTTPConnectionPool,
                                           HTTPSConnectionPool,
                                           HTTPConnection,
                                           HTTPSConnection)
