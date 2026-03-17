from __future__ import annotations

import warnings
from typing import Iterable, Optional

from werkzeug.routing import Map, MapAdapter, Rule

from .wrappers.base import BaseRequestWebsocket


class QuartRule(Rule):
    def __init__(
        self,
        string: str,
        defaults: Optional[dict] = None,
        subdomain: Optional[str] = None,
        methods: Optional[Iterable[str]] = None,
        endpoint: Optional[str] = None,
        strict_slashes: Optional[bool] = None,
        merge_slashes: Optional[bool] = None,
        host: Optional[str] = None,
        websocket: bool = False,
        provide_automatic_options: bool = False,
    ) -> None:
        super().__init__(
            string,
            defaults=defaults,
            subdomain=subdomain,
            methods=methods,
            endpoint=endpoint,
            strict_slashes=strict_slashes,
            merge_slashes=merge_slashes,
            host=host,
            websocket=websocket,
        )
        self.provide_automatic_options = provide_automatic_options


class QuartMap(Map):
    def bind_to_request(
        self, request: BaseRequestWebsocket, subdomain: Optional[str], server_name: Optional[str]
    ) -> MapAdapter:
        host: str
        if server_name is None:
            host = request.host.lower()
        else:
            host = server_name.lower()

        host = _normalise_host(request.scheme, host)

        if subdomain is None and not self.host_matching:
            request_host_parts = _normalise_host(request.scheme, request.host.lower()).split(".")
            config_host_parts = host.split(".")
            offset = -len(config_host_parts)

            if request_host_parts[offset:] != config_host_parts:
                warnings.warn(
                    f"Current server name '{request.host}' doesn't match configured"
                    f" server name '{host}'",
                    stacklevel=2,
                )
                subdomain = "<invalid>"
            else:
                subdomain = ".".join(filter(None, request_host_parts[:offset]))

        if request.root_path:
            try:
                path = request.path.split(request.root_path, 1)[1]
            except IndexError:
                path = " "  # Invalid in paths, hence will result in 404
        else:
            path = request.path

        return super().bind(
            host,
            request.root_path,
            subdomain,
            request.scheme,
            request.method,
            path,
            request.query_string.decode(),
        )


def _normalise_host(scheme: str, host: str) -> str:
    # It is not common to write port 80 or 443 for a hostname,
    # so strip it if present.
    if scheme in {"http", "ws"} and host.endswith(":80"):
        return host[:-3]
    elif scheme in {"https", "wss"} and host.endswith(":443"):
        return host[:-4]
    else:
        return host
