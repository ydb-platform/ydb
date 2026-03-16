# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Service discovery for web exposure.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING


try:
    import aiohttp
    import yarl
except ImportError:
    pass

if TYPE_CHECKING:
    from ..types import Deregisterer
    from .web import MetricsHTTPServer

__all__ = ["ConsulAgent"]


class ConsulAgent:
    """
    Service discovery via a local Consul agent.

    Pass as ``service_discovery`` into
    :func:`prometheus_async.aio.web.start_http_server`/
    :func:`prometheus_async.aio.web.start_http_server_in_thread`.

    :param str name: Application name that is used for the name and the service
        ID if not set.
    :param str service_id: Consul Service ID.  If not set, *name* is used.
    :param tuple tags: Tags to use in Consul registration.
    :param str token: A consul access token.
    :param bool deregister: Whether to deregister when the HTTP server is
        closed.
    """

    def __init__(
        self,
        *,
        name: str = "app-metrics",
        service_id: str | None = None,
        tags: tuple[str, ...] = (),
        token: str | None = None,
        deregister: bool = True,
    ):
        self.name = name
        self.service_id = service_id or name
        self.tags = tags
        self.token = token
        self.deregister = deregister
        self.consul = _LocalConsulAgentClient(token=token)

    async def register(
        self, metrics_server: MetricsHTTPServer
    ) -> Deregisterer | None:
        """
        :return: A coroutine callable to deregister or ``None``.
        """
        resp = await self.consul.register_service(
            name=self.name,
            service_id=self.service_id,
            tags=list(self.tags) or None,
            metrics_server=metrics_server,
        )
        if resp is None:
            return None

        async def deregister() -> None:
            if self.deregister is True:
                await self.consul.deregister_service(self.service_id)

        return deregister


class _LocalConsulAgentClient:  # pragma: no cover -- needs local consul client
    """
    Minimal client to speak to a Consul agent on localhost:8500.
    """

    def __init__(self, token: str | None) -> None:
        self.agent_url = yarl.URL.build(
            scheme="http", host="127.0.0.1", port=8500, path="/v1/agent"
        )

        if token:
            self.headers = {"X-Consul-Token": token}
        else:
            self.headers = {}

        self.session_factory = partial(
            aiohttp.ClientSession, headers=self.headers
        )

    async def get_services(self) -> dict:
        async with self.session_factory() as session:
            resp = await session.get(self.agent_url / "services")
            return await resp.json()

    async def register_service(
        self,
        name: str,
        service_id: str,
        tags: list[str] | None,
        metrics_server: MetricsHTTPServer,
    ) -> aiohttp.ClientResponse | None:
        async with self.session_factory() as session:
            resp = await session.put(
                self.agent_url / "service/register",
                json={
                    "Name": name,
                    "ID": service_id,
                    "Tags": tags,
                    "Address": metrics_server.socket.addr,
                    "Port": metrics_server.socket.port,
                    "Check": {"HTTP": metrics_server.url, "Interval": "10s"},
                },
            )
        if resp.status == 200:
            return resp

        return None

    async def deregister_service(
        self, service_id: str
    ) -> aiohttp.ClientResponse:
        async with self.session_factory() as session:
            return await session.put(
                self.agent_url / "service/deregister" / service_id
            )
