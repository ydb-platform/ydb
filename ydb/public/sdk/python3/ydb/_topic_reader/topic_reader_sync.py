import asyncio
import concurrent.futures
import typing
from typing import List, Union, Optional

from ydb._grpc.grpcwrapper.common_utils import SupportedDriverType
from ydb._topic_common.common import (
    _get_shared_event_loop,
    CallFromSyncToAsync,
    TimeoutType,
)
from ydb._topic_reader import datatypes
from ydb._topic_reader.datatypes import PublicBatch
from ydb._topic_reader.topic_reader import (
    PublicReaderSettings,
    CommitResult,
)
from ydb._topic_reader.topic_reader_asyncio import (
    PublicAsyncIOReader,
    TopicReaderClosedError,
)


class TopicReaderSync:
    _caller: CallFromSyncToAsync
    _async_reader: PublicAsyncIOReader
    _closed: bool
    _parent: typing.Any  # need for prevent stop the client by GC

    def __init__(
        self,
        driver: SupportedDriverType,
        settings: PublicReaderSettings,
        *,
        eventloop: Optional[asyncio.AbstractEventLoop] = None,
        _parent=None,  # need for prevent stop the client by GC
    ):
        self._closed = False

        if eventloop:
            loop = eventloop
        else:
            loop = _get_shared_event_loop()

        self._caller = CallFromSyncToAsync(loop)

        async def create_reader():
            return PublicAsyncIOReader(driver, settings)

        self._async_reader = asyncio.run_coroutine_threadsafe(create_reader(), loop).result()

        self._parent = _parent

    def __del__(self):
        self.close(flush=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def receive_message(self, *, timeout: TimeoutType = None) -> datatypes.PublicMessage:
        """
        Block until receive new message
        It has no async_ version for prevent lost messages, use async_wait_message as signal for new batches available.
        receive_message(timeout=0) may return None even right after async_wait_message() is ok - because lost of partition
        or connection to server lost

        if no new message in timeout seconds (default - infinite): raise TimeoutError()
        if timeout <= 0 - it will fast wait only one event loop cycle - without wait any i/o operations or pauses, get messages from internal buffer only.
        """
        self._check_closed()

        return self._caller.safe_call_with_result(self._async_reader.receive_message(), timeout)

    def async_wait_message(self) -> concurrent.futures.Future:
        """
        Return future, which will completed when the reader has least one message in queue.
        If reader already has message - future will return completed.

        Possible situation when receive signal about message available, but no messages when try to receive a message.
        If message expired between send event and try to retrieve message (for example connection broken).
        """
        self._check_closed()

        return self._caller.unsafe_call_with_future(self._async_reader._reconnector.wait_message())

    def receive_batch(
        self,
        *,
        max_messages: typing.Union[int, None] = None,
        max_bytes: typing.Union[int, None] = None,
        timeout: Union[float, None] = None,
    ) -> Union[PublicBatch, None]:
        """
        Get one messages batch from reader
        It has no async_ version for prevent lost messages, use async_wait_message as signal for new batches available.

        if no new message in timeout seconds (default - infinite): raise TimeoutError()
        if timeout <= 0 - it will fast wait only one event loop cycle - without wait any i/o operations or pauses, get messages from internal buffer only.
        """
        self._check_closed()

        return self._caller.safe_call_with_result(
            self._async_reader.receive_batch(),
            timeout,
        )

    def commit(self, mess: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]):
        """
        Put commit message to internal buffer.

        For the method no way check the commit result
        (for example if lost connection - commits will not re-send and committed messages will receive again)
        """
        self._check_closed()

        self._caller.call_sync(lambda: self._async_reader.commit(mess))

    def commit_with_ack(
        self,
        mess: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch],
        timeout: TimeoutType = None,
    ) -> Union[CommitResult, List[CommitResult]]:
        """
        write commit message to a buffer and wait ack from the server.

        if receive in timeout seconds (default - infinite): raise TimeoutError()
        """
        self._check_closed()

        return self._caller.unsafe_call_with_result(self._async_reader.commit_with_ack(mess), timeout)

    def async_commit_with_ack(
        self, mess: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]
    ) -> concurrent.futures.Future:
        """
        write commit message to a buffer and return Future for wait result.
        """
        self._check_closed()

        return self._caller.unsafe_call_with_future(self._async_reader.commit_with_ack(mess))

    def close(self, *, flush: bool = True, timeout: TimeoutType = None):
        if self._closed:
            return

        self._closed = True

        self._caller.safe_call_with_result(self._async_reader.close(flush), timeout)

    def _check_closed(self):
        if self._closed:
            raise TopicReaderClosedError()
