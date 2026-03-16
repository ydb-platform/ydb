from __future__ import annotations

import warnings
from collections import ChainMap
from typing import TYPE_CHECKING, Any, Optional

import aiohttp

from .channel import Channel, ChannelSubscriber
from .types import SENTINEL


if TYPE_CHECKING:
    from .containers import DockerContainer
    from .docker import Docker


class DockerLog:
    def __init__(self, docker: "Docker", container: "DockerContainer") -> None:
        self.docker = docker
        self.channel = Channel()
        self.container = container
        self.response: Optional[aiohttp.ClientResponse] = None

    def listen(self) -> ChannelSubscriber:
        warnings.warn(
            "use subscribe() method instead", DeprecationWarning, stacklevel=2
        )
        return self.channel.subscribe()

    def subscribe(self) -> ChannelSubscriber:
        return self.channel.subscribe()

    async def run(self, **params: Any) -> None:
        if self.response:
            warnings.warn("already running", RuntimeWarning, stackelevel=2)  # type: ignore
            return
        forced_params = {"follow": True}
        default_params = {"stdout": True, "stderr": True}
        params2 = ChainMap(forced_params, params, default_params)
        # Use infinite timeout for log streaming
        timeout_config = self.docker._resolve_long_running_timeout(SENTINEL)
        try:
            async with self.docker._query(
                f"containers/{self.container._id}/logs",
                params=params2,
                timeout=timeout_config,
            ) as resp:
                self.response = resp
                assert self.response is not None
                while True:
                    msg = await self.response.content.readline()
                    if not msg:
                        break
                    await self.channel.publish(msg)
        except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError):
            pass
        finally:
            # signal termination to subscribers
            await self.channel.publish(None)
            if self.response is not None:
                try:
                    await self.response.release()
                except Exception:
                    pass
            self.response = None

    async def stop(self) -> None:
        if self.response:
            await self.response.release()
