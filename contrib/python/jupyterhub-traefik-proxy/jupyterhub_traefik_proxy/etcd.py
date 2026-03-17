"""Traefik implementation

Custom proxy implementations can subclass :class:`Proxy`
and register in JupyterHub config:

.. sourcecode:: python

    from mymodule import MyProxy
    c.JupyterHub.proxy_class = MyProxy

Route Specification:

- A routespec is a URL prefix ([host]/path/), e.g.
  'host.tld/path/' for host-based routing or '/path/' for default routing.
- Route paths should be normalized to always start and end with '/'
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from tornado.concurrent import run_on_executor
from traitlets import Any, Dict, Unicode, default

from .kv_proxy import TKvProxy
from .traefik_utils import deep_merge


class TraefikEtcdProxy(TKvProxy):
    """JupyterHub Proxy implementation using traefik and etcd"""

    executor = Any()

    provider_name = "etcd"

    etcd_client_kwargs = Dict(
        config=True,
        help="""Extra keyword arguments to pass to the etcd Python client constructor""",
    )

    @default("executor")
    def _default_executor(self):
        return ThreadPoolExecutor(1)

    etcd_url = Unicode(
        "http://127.0.0.1:2379",
        config=True,
        help="URL for the etcd endpoint.",
    )

    etcd_username = Unicode(
        "",
        config=True,
        help="Username for accessing etcd.",
    )
    etcd_password = Unicode(
        "",
        config=True,
        help="Password for accessing etcd.",
    )

    kv_url = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="etcd_url",
    )
    kv_username = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="etcd_username",
    )
    kv_password = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="etcd_password",
    )

    etcd = Any()

    @default("etcd")
    def _default_client(self):
        etcd_service = urlparse(self.etcd_url)
        try:
            import etcd3
        except ImportError:
            raise ImportError(
                "Please install etcd3 or etcdpy package to use traefik-proxy with etcd3"
            )
        kwargs = {
            'host': etcd_service.hostname,
            'port': etcd_service.port,
        }
        if self.etcd_password:
            kwargs.update(
                {
                    "user": self.etcd_username,
                    "password": self.etcd_password,
                }
            )
        if self.etcd_client_kwargs:
            kwargs.update(self.etcd_client_kwargs)
        return etcd3.client(**kwargs)

    def _cleanup(self):
        super()._cleanup()
        self.etcd.close()

    # low-level etcd APIs
    @run_on_executor
    def _etcd_transaction(self, success_actions):
        status, response = self.etcd.transaction(
            compare=[], success=success_actions, failure=[]
        )
        if status != True:
            raise RuntimeError(f"etcd transaction failed: {status}: {response}")
        return response

    @run_on_executor
    def _etcd_get(self, key):
        value, _ = self.etcd.get(key)
        return value

    @run_on_executor
    def _etcd_get_prefix(self, prefix):
        if not prefix.endswith(self.kv_separator):
            prefix += self.kv_separator
        data = list(self.etcd.get_prefix(prefix))
        return data

    # key-value generic methods

    async def _kv_get_tree(self, prefix):
        keys_values = [
            (meta.key.decode("utf8"), value.decode("utf8"))
            for value, meta in await self._etcd_get_prefix(prefix)
        ]
        return self.unflatten_dict_from_kv(keys_values, root_key=prefix)

    async def _kv_atomic_set(self, to_set):
        transactions = []
        for k, v in to_set.items():
            transactions.append(self.etcd.transactions.put(k, v))
        await self._etcd_transaction(transactions)

    async def _kv_atomic_delete(self, *keys):
        """Delete one or more keys from the kv store"""

        transactions = []
        delete = self.etcd.transactions.delete

        for key in keys:
            if key.endswith(self.kv_separator):
                # it's a tree, we have to list sub-keys to delete them atomically
                for _, meta in await self._etcd_get_prefix(key):
                    transactions.append(delete(meta.key))
            else:
                transactions.append(delete(key))
        await self._etcd_transaction(transactions)

    # traefik + etcd methods
    def _setup_traefik_static_config(self):
        self.log.debug("Setting up the etcd provider in the static config")
        url = urlparse(self.etcd_url)
        etcd_config = {
            "endpoints": [url.netloc],
            "rootKey": self.kv_traefik_prefix,
        }
        if url.scheme == "https":
            # If etcd is running over TLS, then traefik needs to know
            etcd_config["tls"] = {}

        if self.etcd_username and self.etcd_password:
            etcd_config.update(
                {
                    "username": self.etcd_username,
                    "password": self.etcd_password,
                }
            )
        self.static_config = deep_merge(
            self.static_config, {"providers": {"etcd": etcd_config}}
        )
        return super()._setup_traefik_static_config()
