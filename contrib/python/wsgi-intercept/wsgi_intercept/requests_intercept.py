"""Intercept HTTP connections that use
`requests <http://docs.python-requests.org/en/latest/>`_.
"""

from requests.packages.urllib3.connectionpool import (HTTPConnectionPool,
        HTTPSConnectionPool)
from requests.packages.urllib3.connection import (HTTPConnection,
        HTTPSConnection)
from ._urllib3 import make_urllib3_override


install, uninstall = make_urllib3_override(HTTPConnectionPool,
                                           HTTPSConnectionPool,
                                           HTTPConnection,
                                           HTTPSConnection)
