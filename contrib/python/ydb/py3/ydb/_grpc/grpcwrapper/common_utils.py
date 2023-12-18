from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import contextvars
import datetime
import functools
import typing
from typing import (
    Optional,
    Any,
    Iterator,
    AsyncIterator,
    Callable,
    Iterable,
    Union,
    Coroutine,
)
from dataclasses import dataclass

import grpc
from google.protobuf.message import Message
from google.protobuf.duration_pb2 import Duration as ProtoDuration
from google.protobuf.timestamp_pb2 import Timestamp as ProtoTimeStamp

import ydb.aio

# Workaround for good IDE and universal for runtime
if typing.TYPE_CHECKING:
    from ..v4.protos import ydb_topic_pb2, ydb_issue_message_pb2
else:
    from ..common.protos import ydb_topic_pb2, ydb_issue_message_pb2

from ... import issues, connection


class IFromProto(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def from_proto(msg: Message) -> Any:
        ...


class IFromProtoWithProtoType(IFromProto):
    @staticmethod
    @abc.abstractmethod
    def empty_proto_message() -> Message:
        ...


class IToProto(abc.ABC):
    @abc.abstractmethod
    def to_proto(self) -> Message:
        ...


class IFromPublic(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def from_public(o: typing.Any) -> typing.Any:
        ...


class IToPublic(abc.ABC):
    @abc.abstractmethod
    def to_public(self) -> typing.Any:
        ...


class UnknownGrpcMessageError(issues.Error):
    pass


_stop_grpc_connection_marker = object()


class QueueToIteratorAsyncIO:
    __slots__ = ("_queue",)

    def __init__(self, q: asyncio.Queue):
        self._queue = q

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is _stop_grpc_connection_marker:
            raise StopAsyncIteration()
        return item


class AsyncQueueToSyncIteratorAsyncIO:
    __slots__ = (
        "_loop",
        "_queue",
    )
    _queue: asyncio.Queue

    def __init__(self, q: asyncio.Queue):
        self._loop = asyncio.get_running_loop()
        self._queue = q

    def __iter__(self):
        return self

    def __next__(self):
        item = asyncio.run_coroutine_threadsafe(self._queue.get(), self._loop).result()
        if item is _stop_grpc_connection_marker:
            raise StopIteration()
        return item


class SyncToAsyncIterator:
    def __init__(self, sync_iterator: Iterator, executor: concurrent.futures.Executor):
        self._sync_iterator = sync_iterator
        self._executor = executor

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            res = await to_thread(self._sync_iterator.__next__, executor=self._executor)
            return res
        except StopIteration:
            raise StopAsyncIteration()


class IGrpcWrapperAsyncIO(abc.ABC):
    @abc.abstractmethod
    async def receive(self) -> Any:
        ...

    @abc.abstractmethod
    def write(self, wrap_message: IToProto):
        ...

    @abc.abstractmethod
    def close(self):
        ...


SupportedDriverType = Union[ydb.Driver, ydb.aio.Driver]


class GrpcWrapperAsyncIO(IGrpcWrapperAsyncIO):
    from_client_grpc: asyncio.Queue
    from_server_grpc: AsyncIterator
    convert_server_grpc_to_wrapper: Callable[[Any], Any]
    _connection_state: str
    _stream_call: Optional[Union[grpc.aio.StreamStreamCall, "grpc._channel._MultiThreadedRendezvous"]]
    _wait_executor: Optional[concurrent.futures.ThreadPoolExecutor]

    def __init__(self, convert_server_grpc_to_wrapper):
        self.from_client_grpc = asyncio.Queue()
        self.convert_server_grpc_to_wrapper = convert_server_grpc_to_wrapper
        self._connection_state = "new"
        self._stream_call = None
        self._wait_executor = None

    def __del__(self):
        self._clean_executor(wait=False)

    async def start(self, driver: SupportedDriverType, stub, method):
        if asyncio.iscoroutinefunction(driver.__call__):
            await self._start_asyncio_driver(driver, stub, method)
        else:
            await self._start_sync_driver(driver, stub, method)
        self._connection_state = "started"

    def close(self):
        self.from_client_grpc.put_nowait(_stop_grpc_connection_marker)
        if self._stream_call:
            self._stream_call.cancel()

        self._clean_executor(wait=True)

    def _clean_executor(self, wait: bool):
        if self._wait_executor:
            self._wait_executor.shutdown(wait)

    async def _start_asyncio_driver(self, driver: ydb.aio.Driver, stub, method):
        requests_iterator = QueueToIteratorAsyncIO(self.from_client_grpc)
        stream_call = await driver(
            requests_iterator,
            stub,
            method,
        )
        self._stream_call = stream_call
        self.from_server_grpc = stream_call.__aiter__()

    async def _start_sync_driver(self, driver: ydb.Driver, stub, method):
        requests_iterator = AsyncQueueToSyncIteratorAsyncIO(self.from_client_grpc)
        self._wait_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        stream_call = await to_thread(driver, requests_iterator, stub, method, executor=self._wait_executor)
        self._stream_call = stream_call
        self.from_server_grpc = SyncToAsyncIterator(stream_call.__iter__(), self._wait_executor)

    async def receive(self) -> Any:
        # todo handle grpc exceptions and convert it to internal exceptions
        try:
            grpc_message = await self.from_server_grpc.__anext__()
        except grpc.RpcError as e:
            raise connection._rpc_error_handler(self._connection_state, e)

        issues._process_response(grpc_message)

        if self._connection_state != "has_received_messages":
            self._connection_state = "has_received_messages"

        # print("rekby, grpc, received", grpc_message)
        return self.convert_server_grpc_to_wrapper(grpc_message)

    def write(self, wrap_message: IToProto):
        grpc_message = wrap_message.to_proto()
        # print("rekby, grpc, send", grpc_message)
        self.from_client_grpc.put_nowait(grpc_message)


@dataclass(init=False)
class ServerStatus(IFromProto):
    __slots__ = ("_grpc_status_code", "_issues")

    def __init__(
        self,
        status: issues.StatusCode,
        issues: Iterable[Any],
    ):
        self.status = status
        self.issues = issues

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def from_proto(
        msg: Union[
            ydb_topic_pb2.StreamReadMessage.FromServer,
            ydb_topic_pb2.StreamWriteMessage.FromServer,
        ]
    ) -> "ServerStatus":
        return ServerStatus(msg.status, msg.issues)

    def is_success(self) -> bool:
        return self.status == issues.StatusCode.SUCCESS

    @classmethod
    def issue_to_str(cls, issue: ydb_issue_message_pb2.IssueMessage):
        res = """code: %s message: "%s" """ % (issue.issue_code, issue.message)
        if len(issue.issues) > 0:
            d = ", "
            res += d + d.join(str(sub_issue) for sub_issue in issue.issues)
        return res


def callback_from_asyncio(callback: Union[Callable, Coroutine]) -> [asyncio.Future, asyncio.Task]:
    loop = asyncio.get_running_loop()

    if asyncio.iscoroutinefunction(callback):
        return loop.create_task(callback())
    else:
        return loop.run_in_executor(None, callback)


async def to_thread(func, *args, executor: Optional[concurrent.futures.Executor], **kwargs):
    """Asynchronously run function *func* in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of *func*.

    copy to_thread from 3.10
    """

    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(executor, func_call)


def proto_duration_from_timedelta(t: Optional[datetime.timedelta]) -> Optional[ProtoDuration]:
    if t is None:
        return None

    res = ProtoDuration()
    res.FromTimedelta(t)


def proto_timestamp_from_datetime(t: Optional[datetime.datetime]) -> Optional[ProtoTimeStamp]:
    if t is None:
        return None

    res = ProtoTimeStamp()
    res.FromDatetime(t)


def datetime_from_proto_timestamp(
    ts: Optional[ProtoTimeStamp],
) -> Optional[datetime.datetime]:
    if ts is None:
        return None
    return ts.ToDatetime()


def timedelta_from_proto_duration(
    d: Optional[ProtoDuration],
) -> Optional[datetime.timedelta]:
    if d is None:
        return None
    return d.ToTimedelta()
