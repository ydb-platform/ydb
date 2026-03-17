import os
from datetime import timedelta
from typing import Any, Optional

from .query import parse_query


DEFAULT_URL = "http://localhost"


class NetboxStorageOpts:
    def __init__(
        self,
        url: str,
        token: str,
        insecure: bool = False,
        exact_host_filter: bool = False,
        threads: int = 1,
        all_hosts_filter: dict[str, list[str]] | None = None,
        cache_path: str = "",
        cache_ttl: timedelta = timedelta(0),
        recache: bool = False,
    ):
        self.url = url
        self.token = token
        self.insecure = insecure
        self.exact_host_filter = exact_host_filter
        self.threads = threads
        self.all_hosts_filter: dict[str, list[str]] = all_hosts_filter or {}
        self.cache_path = cache_path
        self.cache_ttl = cache_ttl
        self.recache = recache

    @classmethod
    def parse_params(cls, conf_params: Optional[dict[str, str]], cli_opts: Any):
        url = os.getenv("NETBOX_URL") or conf_params.get("url") or DEFAULT_URL
        token = os.getenv("NETBOX_TOKEN", "").strip() or conf_params.get("token") or ""
        all_hosts_filter = None
        if all_hosts_filter_env := os.getenv("NETBOX_ALL_HOSTS_FILTER", "").strip():
            all_hosts_filter = parse_query(all_hosts_filter_env.split(","))
        elif all_hosts_filter_params := conf_params.get("all_hosts_filter"):
            all_hosts_filter = all_hosts_filter_params
        threads = os.getenv("NETBOX_CLIENT_THREADS", "").strip() or conf_params.get("threads") or "1"
        insecure = False
        if insecure_env := os.getenv("NETBOX_INSECURE", "").lower():
            insecure = insecure_env in ("true", "1", "t")
        else:
            insecure = bool(conf_params.get("insecure") or False)
        if exact_host_filter_env := os.getenv("NETBOX_EXACT_HOST_FILTER", "").lower():
            exact_host_filter = exact_host_filter_env in ("true", "1", "t")
        else:
            exact_host_filter = bool(conf_params.get("exact_host_filter") or False)

        if cache_path := os.getenv("NETBOX_CACHE_PATH"):
            cache_path = cache_path
        else:
            cache_path = str(conf_params.get("cache_path", ""))

        if cache_ttl := os.getenv("NETBOX_CACHE_TTL"):
            cache_ttl = timedelta(seconds=int(cache_ttl))
        else:
            cache_ttl = timedelta(seconds=conf_params.get("cache_ttl", 0))

        return cls(
            url=url,
            token=token,
            insecure=insecure,
            exact_host_filter=exact_host_filter,
            threads=int(threads),
            all_hosts_filter=all_hosts_filter,
            cache_path=cache_path,
            cache_ttl=cache_ttl,
            recache=cli_opts.recache,
        )
