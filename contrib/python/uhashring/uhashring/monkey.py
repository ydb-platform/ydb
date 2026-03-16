from uhashring import HashRing

__all__ = ["patch_memcache"]


def patch_memcache():
    """Monkey patch python-memcached to implement our consistent hashring
    in its node selection and operations.
    """

    def _init(self, servers, *k, **kw):
        self._old_init(servers, *k, **kw)

        nodes = {}
        for server in self.servers:
            conf = {
                "hostname": server.ip,
                "instance": server,
                "port": server.port,
                "weight": server.weight,
            }
            nodes[server.ip] = conf
        self.uhashring = HashRing(nodes)

    def _get_server(self, key):
        if isinstance(key, tuple):
            return self._old_get_server(key)

        for i in range(self._SERVER_RETRIES):
            for node in self.uhashring.range(key):
                if node["instance"].connect():
                    return node["instance"], key

        return None, None

    memcache = __import__("memcache")
    memcache.Client._old_get_server = memcache.Client._get_server
    memcache.Client._old_init = memcache.Client.__init__
    memcache.Client.__init__ = _init
    memcache.Client._get_server = _get_server
