from __future__ import annotations

import asyncio
import datetime as dt
import warnings
from collections import ChainMap

from .channel import Channel
from .jsonstream import json_stream_stream
from .types import SENTINEL, Sentinel


class DockerEvents:
    def __init__(self, docker):
        self.docker = docker
        self.channel = Channel()
        self.json_stream = None
        self.task = None

    def listen(self):
        warnings.warn(
            "use subscribe() method instead", DeprecationWarning, stacklevel=2
        )
        return self.channel.subscribe()

    def subscribe(self, *, create_task=True, **params):
        """Subscribes to the Docker events channel. Use the keyword argument
        create_task=False to prevent automatically spawning the background
        tasks that listen to the events.

        This function returns a ChannelSubscriber object.
        """
        if create_task and not self.task:
            self.task = asyncio.ensure_future(self.run(**params))
        return self.channel.subscribe()

    def _transform_event(self, data):
        if "time" in data:
            data["time"] = dt.datetime.fromtimestamp(data["time"])
        return data

    async def run(self, *, timeout: float | Sentinel | None = SENTINEL, **params):
        """
        Query the events endpoint of the Docker daemon.

        Publish messages inside the asyncio queue.

        Args:
            timeout: The timeout for the events stream (infinite by default).
        """
        if self.json_stream:
            warnings.warn("already running", category=RuntimeWarning, stacklevel=2)
            return
        forced_params = {"stream": True}
        merged_params = ChainMap(forced_params, params)

        # Default to infinite timeout for event streaming
        timeout_config = self.docker._resolve_long_running_timeout(timeout)

        try:
            async with self.docker._query(
                "events", method="GET", params=merged_params, timeout=timeout_config
            ) as response:
                self.json_stream = json_stream_stream(response, self._transform_event)
                try:
                    async for data in self.json_stream:
                        await self.channel.publish(data)
                finally:
                    if self.json_stream is not None:
                        await self.json_stream._close()
                    self.json_stream = None
        finally:
            # signal termination to subscribers
            await self.channel.publish(None)

    async def stop(self):
        if self.json_stream is not None:
            await self.json_stream._close()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            finally:
                self.task = None
