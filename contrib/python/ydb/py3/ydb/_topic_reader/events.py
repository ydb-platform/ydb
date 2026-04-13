import asyncio
from dataclasses import dataclass
from typing import Awaitable, Optional, Union

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
    start_offset: Optional[int]


class OnInitPartition(BaseReaderEvent):
    pass


class OnShutdownPartition:
    pass


TopicEventDispatchType = Optional[OnPartitionGetStartOffsetResponse]


class EventHandler:
    def on_commit(self, event: OnCommit) -> Union[None, Awaitable[None]]:
        return None

    def on_partition_get_start_offset(
        self,
        event: OnPartitionGetStartOffsetRequest,
    ) -> Union[OnPartitionGetStartOffsetResponse, Awaitable[OnPartitionGetStartOffsetResponse]]:
        return OnPartitionGetStartOffsetResponse(start_offset=None)

    def on_init_partition(self, event: OnInitPartition) -> Union[None, Awaitable[None]]:
        return None

    def on_shutdown_partition(self, event: OnShutdownPartition) -> Union[None, Awaitable[None]]:
        return None

    async def _dispatch(self, event: BaseReaderEvent) -> TopicEventDispatchType:
        if isinstance(event, OnCommit):
            commit_result = self.on_commit(event)
            if asyncio.iscoroutine(commit_result):
                await commit_result
            return None
        elif isinstance(event, OnPartitionGetStartOffsetRequest):
            offset_result = self.on_partition_get_start_offset(event)
            if asyncio.iscoroutine(offset_result):
                return await offset_result
            return offset_result  # type: ignore[return-value]
        elif isinstance(event, OnInitPartition):
            init_result = self.on_init_partition(event)
            if asyncio.iscoroutine(init_result):
                await init_result
            return None
        elif isinstance(event, OnShutdownPartition):
            shutdown_result = self.on_shutdown_partition(event)
            if asyncio.iscoroutine(shutdown_result):
                await shutdown_result
            return None
        else:
            raise ClientInternalError("Unsupported topic reader event")
