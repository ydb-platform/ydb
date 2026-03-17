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

import asyncio
import os
from itertools import chain

from traitlets import Any, Unicode, default, observe

from . import traefik_utils
from .proxy import TraefikProxy


class TraefikFileProviderProxy(TraefikProxy):
    """JupyterHub Proxy implementation using traefik and toml or yaml config file"""

    provider_name = "file"
    mutex = Any()

    @default("mutex")
    def _default_mutex(self):
        return asyncio.Lock()

    dynamic_config_file = Unicode(
        "rules.toml", config=True, help="""traefik's dynamic configuration file"""
    )

    dynamic_config_handler = Any()

    @default("dynamic_config_handler")
    def _default_handler(self):
        return traefik_utils.TraefikConfigFileHandler(self.dynamic_config_file)

    # If dynamic_config_file is changed, then update the dynamic config file handler
    @observe("dynamic_config_file")
    def _set_dynamic_config_file(self, change):
        self.dynamic_config_handler = traefik_utils.TraefikConfigFileHandler(
            self.dynamic_config_file
        )

    @default("dynamic_config")
    def _load_dynamic_config(self):
        try:
            # Load initial dynamic config from disk
            dynamic_config = self.dynamic_config_handler.load()
        except FileNotFoundError:
            dynamic_config = {}

        # fill in default keys
        # use setdefault to ensure these are always fully defined
        # and never _partially_ defined
        http = dynamic_config.setdefault("http", {})
        http.setdefault("services", {})
        http.setdefault("routers", {})
        jupyterhub = dynamic_config.setdefault("jupyterhub", {})
        jupyterhub.setdefault("routes", {})
        return dynamic_config

    def _persist_dynamic_config(self):
        """Save the dynamic config file with the current dynamic_config"""
        # avoid writing empty dicts, which traefik doesn't handle for some reason
        dynamic_config = self.dynamic_config
        if (
            not dynamic_config["http"]["routers"]
            or not dynamic_config["http"]["services"]
        ):
            # traefik can't handle empty dicts, so don't persist them.
            # But don't _remove_ them from our own config
            # I think this is a bug in traefik - empty dicts satisfy the spec
            # use shallow copy, which is cheap but most be done at every level where we modify keys
            dynamic_config = dynamic_config.copy()
            dynamic_config["http"] = http = dynamic_config["http"].copy()
            for key in ("routers", "services"):
                if not http[key]:
                    http.pop(key)
            if not http:
                dynamic_config.pop("http")
        self.log.debug("Writing dynamic config %s", dynamic_config)
        self.dynamic_config_handler.atomic_dump(dynamic_config)

    async def _setup_traefik_dynamic_config(self):
        self.log.info(
            f"Creating the dynamic configuration file: {self.dynamic_config_file}"
        )
        await super()._setup_traefik_dynamic_config()

    async def _setup_traefik_static_config(self):
        self.static_config["providers"] = {
            "file": {"filename": self.dynamic_config_file, "watch": True}
        }
        await super()._setup_traefik_static_config()

    def _cleanup(self):
        """Cleanup dynamic config file as well"""
        super()._cleanup()
        try:
            os.remove(self.dynamic_config_file)
        except Exception as e:
            self.log.error(
                f"Failed to remove traefik configuration file {self.dynamic_config_file}: {e}"
            )

    async def _get_jupyterhub_dynamic_config(self):
        return self.dynamic_config["jupyterhub"]

    async def _apply_dynamic_config(self, traefik_config, jupyterhub_config=None):
        dynamic_config = {}
        dynamic_config.update(traefik_config)
        # file provider stores jupyterhub info in "jupyterhub" key
        if jupyterhub_config is not None:
            dynamic_config["jupyterhub"] = jupyterhub_config
        async with self.mutex:
            self.dynamic_config = traefik_utils.deep_merge(
                self.dynamic_config, dynamic_config
            )
            self._persist_dynamic_config()

    async def _delete_dynamic_config(self, traefik_keys, jupyterhub_keys):
        """Delete keys from dynamic configuration

        jupyterhub dynamic config is _inside_ traefik dynamic config,
        under the 'jupyterhub' key
        """
        jupyterhub_keys = (["jupyterhub"] + key_path for key_path in jupyterhub_keys)
        async with self.mutex:
            for key_path in chain(traefik_keys, jupyterhub_keys):
                parent = self.dynamic_config
                for key in key_path[:-1]:
                    if key in parent:
                        parent = parent[key]
                    else:
                        parent = {}
                        break

                # final key, time to delete
                key = key_path[-1]
                if key in parent:
                    parent.pop(key)
                else:
                    self.log.warning(
                        f"Missing dynamic config, nothing to delete: {'.'.join(key_path)}"
                    )

            self._persist_dynamic_config()

    async def get_route(self, routespec):
        """Return the route info for a given routespec.

        Args:
            routespec (str):
                A URI that was used to add this route,
                e.g. `host.tld/path/`

        Returns:
            result (dict):
                dict with the following keys::

                'routespec': The normalized route specification passed in to add_route
                    ([host]/path/)
                'target': The target host for this route (proto://host)
                'data': The arbitrary data dict that was passed in by JupyterHub when adding this
                        route.

            None: if there are no routes matching the given routespec
        """
        routespec = self.validate_routespec(routespec)
        router_alias = traefik_utils.generate_alias(routespec, "router")
        async with self.mutex:
            route = self.dynamic_config["jupyterhub"]["routes"].get(router_alias)
            if not route:
                return None
            return {
                "routespec": route["routespec"],
                "data": route["data"],
                "target": route["target"],
            }
