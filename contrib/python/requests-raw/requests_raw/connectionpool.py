from .connection import RawHTTPConnection, RawHTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool


class RawHTTPConnectionPool(HTTPConnectionPool):
    ConnectionCls = RawHTTPConnection


class RawHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = RawHTTPSConnection
