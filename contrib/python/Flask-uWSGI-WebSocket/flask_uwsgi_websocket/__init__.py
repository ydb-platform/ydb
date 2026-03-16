'''
Flask-uWSGI-WebSocket
---------------------
High-performance WebSockets for your Flask apps powered by `uWSGI <http://uwsgi-docs.readthedocs.org/en/latest/>`_.
'''

__docformat__ = 'restructuredtext'
__version__ = '0.6.1'
__license__ = 'MIT'
__author__  = 'Zach Kelling'

import sys

from ._async import *
from ._uwsgi import uwsgi
from .websocket import *

class GeventNotInstalled(Exception):
    pass

try:
    from ._gevent import *
except ImportError:
    class GeventWebSocket(object):
        def __init__(self, *args, **kwargs):
            raise GeventNotInstalled("Gevent must be installed to use GeventWebSocket. Try: `pip install gevent`.")

class AsyncioNotAvailable(Exception):
    pass

try:
    assert sys.version_info > (3,4)
    from ._asyncio import *
except (AssertionError, ImportError):
    class AsyncioWebSocket(object):
        def __init__(self, *args, **kwargs):
            raise AsyncioNotAvailable("Asyncio should be enabled at uwsgi compile time. Try: `UWSGI_PROFILE=asyncio pip install uwsgi`.")
