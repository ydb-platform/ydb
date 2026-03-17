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

import base64
import string
from urllib.parse import urlparse

from traitlets import Any, Dict, Unicode, default

from .kv_proxy import TKvProxy
from .traefik_utils import deep_merge


class TraefikConsulProxy(TKvProxy):
    """JupyterHub Proxy implementation using traefik and Consul"""

    provider_name = "consul"

    # Consul doesn't accept keys containing // or starting with / so we have to escape them
    key_safe_chars = string.ascii_letters + string.digits + "!@#$%^&*();<>-.+?:"

    consul_client_kwargs = Dict(
        config=True,
        help="Extra consul client constructor arguments",
    )

    consul_url = Unicode(
        "http://127.0.0.1:8500",
        config=True,
        help="URL for the consul endpoint.",
    )
    consul_username = Unicode(
        "",
        config=True,
        help="Usrname for accessing consul.",
    )
    consul_password = Unicode(
        "",
        config=True,
        help="Password or token for accessing consul.",
    )

    kv_url = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="consul_url",
    )
    kv_username = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="consul_username",
    )
    kv_password = Unicode("DEPRECATED", config=True).tag(
        deprecated_in="1.0",
        deprecated_for="consul_password",
    )

    consul = Any()

    @default("consul")
    def _default_client(self):
        try:
            import consul.aio
        except ImportError:
            raise ImportError(
                "Please install python-consul2 package to use traefik-proxy with consul"
            )
        consul_service = urlparse(self.consul_url)
        kwargs = {
            "host": consul_service.hostname,
            "port": consul_service.port,
        }
        if self.consul_password:
            kwargs.update({"token": self.consul_password})
        if self.consul_client_kwargs:
            kwargs.update(self.consul_client_kwargs)
        return consul.aio.Consul(**kwargs)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.log.warning(
            "Using traefik with consul is deprecated in jupyterhub-traefik-proxy 1.0 due to lack of support for the python-consul2 API client. Use etcd instead."
        )

    def _setup_traefik_static_config(self):
        provider_config = {
            "consul": {
                "rootKey": self.kv_traefik_prefix,
                "endpoints": [urlparse(self.consul_url).netloc],
            }
        }

        self.static_config = deep_merge(
            self.static_config, {"providers": provider_config}
        )
        return super()._setup_traefik_static_config()

    def _start_traefik(self):
        if self.consul_password:
            if self.consul_username:
                self.traefik_env.setdefault(
                    "CONSUL_HTTP_AUTH", f"{self.consul_username}:{self.consul_password}"
                )
            else:
                self.traefik_env.setdefault("CONSUL_HTTP_TOKEN", self.consul_password)
        super()._start_traefik()

    async def _kv_atomic_set(self, to_set):
        payload = []

        def append_payload(key, val):
            payload.append(
                {
                    "KV": {
                        "Verb": "set",
                        "Key": key,
                        "Value": base64.b64encode(val.encode()).decode(),
                    }
                }
            )

        for k, v in to_set.items():
            append_payload(k, v)

        try:
            await self.consul.txn.put(payload=payload)
        except Exception:
            self.log.exception("Error uploading payload to KV store!")
        else:
            self.log.debug("Successfully uploaded payload to KV store")

    async def _kv_atomic_delete(self, *to_delete):
        payload = []

        for key in to_delete:
            if key.endswith(self.kv_separator):
                verb = "delete-tree"
            else:
                verb = "delete"
            payload.append(
                {
                    "KV": {"Verb": verb, "Key": key},
                }
            )
        status, response = await self.consul.txn.put(payload=payload)
        # check response?

    async def _kv_get_tree(self, prefix):
        response = await self.consul.txn.put(
            payload=[
                {
                    "KV": {
                        "Verb": "get-tree",
                        "Key": prefix,
                    }
                }
            ]
        )
        kv_list = [
            (
                item["KV"]["Key"],
                base64.b64decode(item["KV"]["Value"] or '').decode("utf8"),
            )
            for item in response["Results"]
        ]
        return self.unflatten_dict_from_kv(kv_list, root_key=prefix)
