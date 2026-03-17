from __future__ import annotations

import asyncio


class ChannelSubscriber:
    def __init__(self, channel):
        self.queue = asyncio.Queue()
        self.channel = channel
        self.channel.queues.append(self.queue)

    def __del__(self):
        self.channel.queues.remove(self.queue)

    async def get(self):
        x = await self.queue.get()
        return x


class Channel:
    def __init__(self):
        self.queues = []

    async def publish(self, obj) -> None:
        for el in self.queues:
            await el.put(obj)

    def subscribe(self) -> ChannelSubscriber:
        return ChannelSubscriber(self)
