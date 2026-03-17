from ipaddress import ip_address, ip_network
from typing import Awaitable, Callable, Optional, Set

import aiohttp
from aiohttp import web

from .abc import ABC
from .exceptions import IPNetwork
from .log import logger


class Cloudflare(ABC):
    def __init__(self, client: Optional[aiohttp.ClientSession] = None) -> None:
        self._ip_networks: Set[IPNetwork] = set()
        self._client = client

    def _parse_mask(self, text: str) -> Set[IPNetwork]:
        ret = set()
        for mask in text.splitlines():
            try:
                real_mask = ip_network(mask)
            except (ValueError, TypeError):
                continue

            ret.add(real_mask)
        return ret

    async def setup(self, app: web.Application) -> None:
        if self._client is not None:  # pragma: no branch
            client = self._client
        else:
            client = aiohttp.ClientSession()  # pragma: no cover
        try:
            async with client.get("https://www.cloudflare.com/ips-v4") as response:
                self._ip_networks |= self._parse_mask(await response.text())
            async with client.get("https://www.cloudflare.com/ips-v6") as response:
                self._ip_networks |= self._parse_mask(await response.text())
        finally:
            if self._client is None:  # pragma: no cover
                await client.close()

        if not self._ip_networks:
            raise RuntimeError("No networks are available")
        app.middlewares.append(self.middleware)

    @web.middleware
    async def middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        remote = request.remote
        assert remote is not None, "HTTP transport is closed"
        remote_ip = ip_address(remote)

        for network in self._ip_networks:
            if remote_ip in network:
                request = request.clone(remote=request.headers["CF-CONNECTING-IP"])
                return await handler(request)

        msg = "Not cloudflare: %(remote_ip)s"
        context = {"remote_ip": remote_ip}
        logger.error(msg, context)

        return await self.raise_error(request)
