"""Redis backend"""

import asyncio
from functools import partial
from urllib.parse import urlparse

from traitlets import Any, Dict, Unicode, default

from .kv_proxy import TKvProxy
from .traefik_utils import deep_merge


class TraefikRedisProxy(TKvProxy):
    """JupyterHub Proxy implementation using traefik and redis"""

    provider_name = "redis"

    redis_url = Unicode(
        'redis://localhost:6379', config=True, help="""The URL for the redis endpoint"""
    )
    redis_username = Unicode(config=True, help="The redis username")
    redis_password = Unicode(config=True, help="The redis password")

    redis_client_kwargs = Dict(
        config=True,
        help="Additional keyword arguments to pass through to the `redis.asyncio.Redis` constructor",
    )

    redis = Any()

    @default("redis")
    def _connect_redis(self):
        try:
            from redis.asyncio import Redis
        except ImportError:
            raise ImportError(
                "Please install `redis` package to use traefik-proxy with redis"
            )

        url = urlparse(self.redis_url)
        if url.port:
            port = url.port
        else:
            # default port
            port = 6379
        kwargs = dict(
            host=url.hostname,
            port=port,
            decode_responses=True,
        )
        if not any(key.startswith('retry') for key in self.redis_client_kwargs):
            # default retry configuration, if no retry configuration provided

            from redis.asyncio.retry import Retry
            from redis.backoff import ExponentialBackoff
            from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError

            # works out to ~30 seconds, e.g. handling redis server restart
            # sum(ExponentialBackoff(cap=5).compute(i) for i in range(15))
            kwargs["retry"] = Retry(ExponentialBackoff(cap=5), 15)
            kwargs["retry_on_error"] = [BusyLoadingError, ConnectionError, TimeoutError]
        if self.redis_password:
            kwargs["password"] = self.redis_password
        if self.redis_username:
            kwargs["username"] = self.redis_username
        kwargs.update(self.redis_client_kwargs)
        return Redis(**kwargs)

    async def _cleanup(self):
        f = super()._cleanup()
        if f is not None:
            await f
        if hasattr(self.redis, 'aclose'):
            aclose = self.redis.aclose
        else:
            # redis < 5.0.1
            aclose = self.redis.close
        await aclose()

    def _setup_traefik_static_config(self):
        self.log.debug("Setting up the redis provider in the traefik static config")
        url = urlparse(self.redis_url)
        redis_config = {
            "endpoints": [url.netloc],
            "rootKey": self.kv_traefik_prefix,
        }
        if self.redis_username:
            redis_config["username"] = self.redis_username
        if self.redis_password:
            redis_config["password"] = self.redis_password

        self.static_config = deep_merge(
            self.static_config, {"providers": {"redis": redis_config}}
        )
        return super()._setup_traefik_static_config()

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
        self.log.debug("Setting redis keys %s", to_set.keys())
        await self.redis.mset(to_set)

    _delete_script = Any()

    @default("_delete_script")
    def _register_delete_script(self):
        """Register LUA script for deleting all keys matching in a prefix

        Doing the scan & delete from Python is _extremely_ slow
        for some reason
        """
        _delete_lua = """
        local all_keys = {};
        local cursor = "0";
        repeat
            local result = redis.call("SCAN", cursor, "match", ARGV[1], "count", ARGV[2])
            cursor = result[1];
            for i, key in ipairs(result[2]) do
                table.insert(all_keys, key);
            end
        until cursor == "0"
        for i, key in ipairs(all_keys) do
            redis.call("DEL", key);
        end
        return #all_keys;
        """
        return self.redis.register_script(_delete_lua)

    async def _kv_atomic_delete(self, *keys):
        """Delete one or more keys

        If a key ends with `self.kv_separator`, it should be a recursive delete
        """

        to_delete = []

        futures = []
        for key in keys:
            if key.endswith(self.kv_separator):
                prefix = key + "*"
                self.log.debug("Deleting redis tree %s", prefix)
                f = asyncio.ensure_future(self._delete_script(args=[prefix, 100]))

                def _log_delete(_prefix, f):
                    self.log.debug("Deleted %i keys in %s", f.result(), _prefix)

                f.add_done_callback(partial(_log_delete, prefix))
                futures.append(f)
            else:
                to_delete.append(key)

        if to_delete:
            self.log.debug("Deleting redis keys %s", to_delete)
            futures.append(self.redis.delete(*to_delete))

        await asyncio.gather(*futures)

    async def _kv_get_tree(self, prefix):
        """Return all data under prefix as a dict"""
        if not prefix.endswith(self.kv_separator):
            prefix = prefix + self.kv_separator

        keys = []
        # is there a possibility this could get too big?
        # should we batch?
        async for key in self.redis.scan_iter(match=prefix + "*"):
            keys.append(key)
        self.log.debug("Getting redis keys %s", keys)
        values = list(await self.redis.mget(keys))
        kv_list = zip(keys, values)
        return self.unflatten_dict_from_kv(kv_list, root_key=prefix)
