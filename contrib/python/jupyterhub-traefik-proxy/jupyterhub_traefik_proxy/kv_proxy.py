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
from collections.abc import Mapping
from functools import wraps
from numbers import Number

from traitlets import Unicode

from . import traefik_utils
from .proxy import TraefikProxy


def _one_at_a_time(method):
    """decorator to limit an async method to be called only once

    If multiple concurrent calls to this method are made,
    piggy-back on the outstanding call instead of queuing
    or letting requests pile up.
    """

    @wraps(method)
    async def locked_method(*args, **kwargs):
        if getattr(method, "_shared_future", None) is not None:
            f = method._shared_future
            if f.done():
                method._shared_future = None
            else:
                return await f

        method._shared_future = f = asyncio.ensure_future(method(*args, **kwargs))
        try:
            return await f
        finally:
            method._shared_future = None

    return locked_method


class TKvProxy(TraefikProxy):
    """
    JupyterHub Proxy implementation using traefik and a key-value store.

    Custom proxy implementations based on traefik and a key-value store
    can sublass :class:`TKvProxy`.
    """

    kv_traefik_prefix = traefik_utils.KVStorePrefix(
        "traefik",
        config=True,
        help="""The key value store key prefix for traefik static configuration""",
    )

    kv_jupyterhub_prefix = traefik_utils.KVStorePrefix(
        "jupyterhub",
        config=True,
        help="""The key value store key prefix for traefik dynamic configuration""",
    )

    kv_separator = Unicode(
        "/",
        config=True,
        help="""The separator used for the path in the KV store""",
    )

    # these should be the only three methods a KV provider needs to define

    async def _kv_atomic_set(self, to_set: dict):
        """Set a collection of keys and values

        Should be done atomically (i.e. in a transaction),
        setting nothing on failure.

        Args:

        to_set (dict): key/value pairs to set
            Will always be a flattened dict
            of single key-value pairs,
            not a nested structure.
        """
        raise NotImplementedError()

    async def _kv_atomic_delete(self, *keys):
        """Delete one or more keys

        If a key ends with `self.kv_separator`, it should be a recursive delete
        """
        raise NotImplementedError()

    async def _kv_get_tree(self, prefix):
        """Return all data under prefix as a dict

        Should probably use `unflatten_dict_from_kv`
        """
        raise NotImplementedError()

    # now: implement methods required by TraefikProxy base class

    async def _apply_dynamic_config(self, dynamic_config, jupyterhub_config=None):
        """Apply dynamic config (and optional jupyterhub info) atomically"""
        to_set = self.flatten_dict_for_kv(dynamic_config, prefix=self.kv_traefik_prefix)
        if jupyterhub_config:
            to_set.update(
                self.flatten_dict_for_kv(
                    jupyterhub_config, prefix=self.kv_jupyterhub_prefix
                )
            )
        self.log.debug("Setting key-value config %s", to_set)
        await self._kv_atomic_set(to_set)

    async def _delete_dynamic_config(self, traefik_keys, jupyterhub_keys):
        """Delete keys from dynamic configuration

        Translate key paths to flat kv keys
        """
        to_delete = [
            self.kv_separator.join([self.kv_traefik_prefix] + key_path + [""])
            for key_path in traefik_keys
        ]
        to_delete.extend(
            self.kv_separator.join([self.kv_jupyterhub_prefix] + key_path + [""])
            for key_path in jupyterhub_keys
        )
        async with self.semaphore:
            try:
                await self._kv_atomic_delete(*to_delete)
            except Exception as e:
                self.log.error("Couldn't delete config %s: %s", to_delete, e)
                raise

    @_one_at_a_time
    async def _get_jupyterhub_dynamic_config(self):
        """jupyterhub data is in our kv store"""
        return await self._kv_get_tree(self.kv_jupyterhub_prefix)

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
        route_key = self.kv_separator.join(
            [self.kv_jupyterhub_prefix, "routes", router_alias]
        )
        route = await self._kv_get_tree(route_key)
        if not route:
            return None
        return {key: route[key] for key in ("routespec", "data", "target")}

    # deep/flat dict translation

    def _kv_to_str(self, value):
        """KV stores only store strings

        serialize numbers and booleans to strings that traefik will recognize
        """
        if isinstance(value, str):
            return value
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, Number):
            return str(value)
        else:
            # don't coerce unrecognized types, TypeError will be raised later
            return value

    def flatten_dict_for_kv(self, data, prefix=''):
        """Flatten a dictionary of `data` for storage in the KV store,
        prefixing each key with `prefix` and joining each key with
        :attr:`TKvProxy.kv_separator`.

        If the final value is a :class:`list`, then the provided bottom-level key
        shall be appended with an incrementing numeric number, in the style
        that is used by traefik's KV store, e.g.

        .. code-block::

            flatten_dict_for_kv({
                'x' : {
                    'y' : {
                        'z': 'a'
                    }
                }, {
                    'foo': 'bar'
                },
                'baz': [ 'a', 'b', 'c' ]
            })

        :return: The flattened dictionary
        :rtype: dict

        e.g.

        .. code-block::

            {
                 'traefik/x/y/z' : 'a',
                 'traefik/x/foo': 'bar',
                 'traefik/baz/0': 'a',
                 'traefik/baz/1': 'b',
                 'traefik/baz/2': 'c',
            }

        Inspired by `this answer on StackOverflow <https://stackoverflow.com/a/6027615>`_
        """
        sep = self.kv_separator
        # if it's a list, cast to dict of str keys, i.e. ["x"] -> {"0": "x"}
        # if isinstance(data, list):
        #     data = {str(i): item for i, item in enumerate(data)}

        items = {}
        for k, v in data.items():
            if prefix:
                new_key = f"{prefix}{sep}{k}"
            else:
                new_key = k

            if isinstance(v, list):
                # if it's a list, cast to dict of str keys, i.e. ["x"] -> {"0": "x"}
                v = {str(i): item for i, item in enumerate(v)}

            # two cases:
            # 1. Mapping (dict)
            # 2. scalar (cast to str)
            if isinstance(v, Mapping):
                if not v:
                    self.log.warning(
                        f"Not setting anything for empty dict at {new_key}"
                    )
                items.update(self.flatten_dict_for_kv(v, prefix=new_key))
            else:
                # cast _known_ types to str
                v = self._kv_to_str(v)
                # if cast didn't coerce, we can't handle it
                if not isinstance(v, str):
                    raise ValueError(
                        f"Cannot upload {new_key}: {v} of type {type(v)} to kv store"
                    )
                items[new_key] = v
        return items

    def unflatten_dict_from_kv(self, kv_list, root_key=""):
        """Reconstruct tree dict from list of key/value pairs

        This is the inverse of flatten_dict_for_kv,
        not including str coercion.

        Args:

        kv_list (list):
            list of (key, value) pairs.
            keys and values should all be strings.
        root_key (str, optional):
            The key representing the root of the tree,
            if not the root of the key-value store.

        Returns:

        tree (dict):
            The reconstructed dictionary.
            All values will still be strings,
            even those that originated as numbers or booleans.
        """

        sep = self.kv_separator

        def by_depth(item):
            """sort-key function to get shallow items first

            Ensures

            So that we know that we always see an item
            before its children
            """
            key, value = item
            key_path = key.split(sep)
            for i, label in enumerate(key_path):
                # ensure
                if label.isdigit():
                    key_path[i] = int(label)
            return len(key.split(sep))

        tree = {}
        for key, value in sorted(kv_list, key=by_depth):
            key_path = key.split(sep)
            d = tree
            for parent_key, key in zip(key_path[:-1], key_path[1:]):
                if parent_key.isdigit():
                    parent_key = int(parent_key)
                if isinstance(d, dict) and parent_key not in d:
                    # create container
                    if key.isdigit():
                        # integer keys mean it's a list
                        d[parent_key] = []
                    else:
                        d[parent_key] = {}
                elif isinstance(d, list):
                    if key.isdigit():
                        # integer keys mean it's a list
                        next_d = []
                    else:
                        next_d = {}
                    d.append(next_d)
                # walk down to the next level
                d = d[parent_key]
            if isinstance(d, list):
                # validate list keys
                if len(d) != int(key):
                    raise IndexError(
                        f"Got invalid list key {key_path}, missing previous items in {d}"
                    )
                d.append(value)
            else:
                d[key] = value

        if root_key:
            original_tree = tree
            # get the root of the tree,
            # rather than starting from root
            for key in root_key.strip(self.kv_separator).split(self.kv_separator):
                if key not in tree:
                    self.log.warning(
                        f"Root key {root_key!r} not found in {original_tree}"
                    )
                    return {}
                tree = tree[key]
        return tree
