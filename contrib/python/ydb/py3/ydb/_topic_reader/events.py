import asyncio
from dataclasses import dataclass
from typing import Awaitable, Union

from ..issues import ClientInternalError

__all__ = [
    "OnCommit",
    "OnPartitionGetStartOffsetRequest",
    "OnPartitionGetStartOffsetResponse",
    "OnInitPartition",
    "OnShutdownPartition",
    "EventHandler",
]


class BaseReaderEvent:
    pass


@dataclass
class OnCommit(BaseReaderEvent):
    topic: str
    offset: int


@dataclass
class OnPartitionGetStartOffsetRequest(BaseReaderEvent):
    topic: str
    partition_id: int


@dataclass
class OnPartitionGetStartOffsetResponse:
    start_offset: int


class OnInitPartition(BaseReaderEvent):
    pass


class OnShutdownPartition:
    pass


TopicEventDispatchType = Union[OnPartitionGetStartOffsetResponse, None]


class EventHandler:
    def on_commit(self, event: OnCommit) -> Union[None, Awaitable[None]]:
        pass

    def on_partition_get_start_offset(
        self,
        event: OnPartitionGetStartOffsetRequest,
    ) -> Union[OnPartitionGetStartOffsetResponse, Awaitable[OnPartitionGetStartOffsetResponse]]:
        pass

    def on_init_partition(self, event: OnInitPartition) -> Union[None, Awaitable[None]]:
        pass

    def on_shutdown_partition(self, event: OnShutdownPartition) -> Union[None, Awaitable[None]]:
        pass

    async def _dispatch(self, event: BaseReaderEvent) -> Awaitable[TopicEventDispatchType]:
        f = None
        if isinstance(event, OnCommit):
            f = self.on_commit
        elif isinstance(event, OnPartitionGetStartOffsetRequest):
            f = self.on_partition_get_start_offset
        elif isinstance(event, OnInitPartition):
            f = self.on_init_partition
        elif isinstance(event, OnShutdownPartition):
            f = self.on_shutdown_partition
        else:
            raise ClientInternalError("Unsupported topic reader event")

        if asyncio.iscoroutinefunction(f):
            return await f(event)

        return f(event)
