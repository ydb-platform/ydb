import asyncio
import concurrent.futures
import typing
from typing import List, Union, Iterable, Optional

from ydb._grpc.grpcwrapper.common_utils import SupportedDriverType
from ydb._topic_common.common import (
    _get_shared_event_loop,
    CallFromSyncToAsync,
    TimeoutType,
)
from ydb._topic_reader import datatypes
from ydb._topic_reader.datatypes import PublicMessage, PublicBatch, ICommittable
from ydb._topic_reader.topic_reader import (
    PublicReaderSettings,
    SessionStat,
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

    def __init__(
        self,
        driver: SupportedDriverType,
        settings: PublicReaderSettings,
        *,
        eventloop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._closed = False

        if eventloop:
            loop = eventloop
        else:
            loop = _get_shared_event_loop()

        self._caller = CallFromSyncToAsync(loop)

        async def create_reader():
            return PublicAsyncIOReader(driver, settings)

        self._async_reader = asyncio.run_coroutine_threadsafe(
            create_reader(), loop
        ).result()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def async_sessions_stat(self) -> concurrent.futures.Future:
        """
        Receive stat from the server, return feature.
        """
        raise NotImplementedError()

    async def sessions_stat(self) -> List[SessionStat]:
        """
        Receive stat from the server

        use async_sessions_stat for set explicit wait timeout
        """
        raise NotImplementedError()

    def messages(
        self, *, timeout: Union[float, None] = None
    ) -> Iterable[PublicMessage]:
        """
        todo?

        Block until receive new message
        It has no async_ version for prevent lost messages, use async_wait_message as signal for new batches available.

        if no new message in timeout seconds (default - infinite): stop iterations by raise StopIteration
        if timeout <= 0 - it will fast non block method, get messages from internal buffer only.
        """
        raise NotImplementedError()

    def receive_message(self, *, timeout: Union[float, None] = None) -> PublicMessage:
        """
        Block until receive new message
        It has no async_ version for prevent lost messages, use async_wait_message as signal for new batches available.

        if no new message in timeout seconds (default - infinite): raise TimeoutError()
        if timeout <= 0 - it will fast non block method, get messages from internal buffer only.
        """
        raise NotImplementedError()

    def async_wait_message(self) -> concurrent.futures.Future:
        """
        Return future, which will completed when the reader has least one message in queue.
        If reader already has message - future will return completed.

        Possible situation when receive signal about message available, but no messages when try to receive a message.
        If message expired between send event and try to retrieve message (for example connection broken).
        """
        raise NotImplementedError()

    def batches(
        self,
        *,
        max_messages: Union[int, None] = None,
        max_bytes: Union[int, None] = None,
        timeout: Union[float, None] = None,
    ) -> Iterable[PublicBatch]:
        """
        Block until receive new batch.
        It has no async_ version for prevent lost messages, use async_wait_message as signal for new batches available.

        if no new message in timeout seconds (default - infinite): stop iterations by raise StopIteration
        if timeout <= 0 - it will fast non block method, get messages from internal buffer only.
        """
        raise NotImplementedError()

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
        if timeout <= 0 - it will fast non block method, get messages from internal buffer only.
        """
        self._check_closed()

        return self._caller.safe_call_with_result(
            self._async_reader.receive_batch(
                max_messages=max_messages, max_bytes=max_bytes
            ),
            timeout,
        )

    def commit(
        self, mess: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]
    ):
        """
        Put commit message to internal buffer.

        For the method no way check the commit result
        (for example if lost connection - commits will not re-send and committed messages will receive again)
        """
        self._check_closed()

        self._caller.call_sync(self._async_reader.commit(mess))

    def commit_with_ack(
        self, mess: ICommittable, timeout: TimeoutType = None
    ) -> Union[CommitResult, List[CommitResult]]:
        """
        write commit message to a buffer and wait ack from the server.

        if receive in timeout seconds (default - infinite): raise TimeoutError()
        """
        self._check_closed()

        return self._caller.unsafe_call_with_result(
            self._async_reader.commit_with_ack(mess), timeout
        )

    def async_commit_with_ack(
        self, mess: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]
    ) -> concurrent.futures.Future:
        """
        write commit message to a buffer and return Future for wait result.
        """
        self._check_closed()

        return self._caller.unsafe_call_with_future(
            self._async_reader.commit_with_ack(mess)
        )

    def async_flush(self) -> concurrent.futures.Future:
        """
        force send all commit messages from internal buffers to server and return Future for wait server acks.
        """
        raise NotImplementedError()

    def flush(self):
        """
        force send all commit messages from internal buffers to server and wait acks for all of them.
        """
        raise NotImplementedError()

    def close(self, *, timeout: TimeoutType = None):
        if self._closed:
            return

        self._closed = True

        self._caller.safe_call_with_result(self._async_reader.close(), timeout)

    def _check_closed(self):
        if self._closed:
            raise TopicReaderClosedError()
