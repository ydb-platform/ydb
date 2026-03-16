from requests.adapters import HTTPAdapter
from requests.utils import get_auth_from_url
from requests.exceptions import InvalidSchema
from urllib3.poolmanager import proxy_from_url
from .connectionpool import RawHTTPConnectionPool, RawHTTPSConnectionPool

try:
    from .socks import RawSOCKSProxyManager
except ImportError:

    def RawSOCKSProxyManager(*args, **kwargs):
        raise InvalidSchema("Missing dependencies for SOCKS support.")


class RawAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super(RawAdapter, self).__init__(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme["http"] = RawHTTPConnectionPool
        self.poolmanager.key_fn_by_scheme["http"] = self.poolmanager.key_fn_by_scheme["http"]

        self.poolmanager.pool_classes_by_scheme["https"] = RawHTTPSConnectionPool
        self.poolmanager.key_fn_by_scheme["https"] = self.poolmanager.key_fn_by_scheme["https"]

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        if proxy in self.proxy_manager:
            manager = self.proxy_manager[proxy]
        elif proxy.lower().startswith("socks"):
            username, password = get_auth_from_url(proxy)
            manager = self.proxy_manager[proxy] = RawSOCKSProxyManager(
                proxy,
                username=username,
                password=password,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )
        else:
            proxy_headers = self.proxy_headers(proxy)
            manager = self.proxy_manager[proxy] = proxy_from_url(
                proxy,
                proxy_headers=proxy_headers,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )

        return manager
