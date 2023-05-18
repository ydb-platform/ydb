from __future__ import annotations

import asyncio
import typing
from concurrent.futures import Future
from typing import Union, List, Optional

from .._grpc.grpcwrapper.common_utils import SupportedDriverType
from .topic_writer import (
    PublicWriterSettings,
    PublicWriterInitInfo,
    PublicWriteResult,
    Message,
    TopicWriterClosedError,
)

from .topic_writer_asyncio import WriterAsyncIO
from .._topic_common.common import (
    _get_shared_event_loop,
    TimeoutType,
    CallFromSyncToAsync,
)


class WriterSync:
    _caller: CallFromSyncToAsync
    _async_writer: WriterAsyncIO
    _closed: bool
    _parent: typing.Any  # need for prevent close parent client by GC

    def __init__(
        self,
        driver: SupportedDriverType,
        settings: PublicWriterSettings,
        *,
        eventloop: Optional[asyncio.AbstractEventLoop] = None,
        _parent=None,
    ):

        self._closed = False

        if eventloop:
            loop = eventloop
        else:
            loop = _get_shared_event_loop()

        self._caller = CallFromSyncToAsync(loop)

        async def create_async_writer():
            return WriterAsyncIO(driver, settings)

        self._async_writer = self._caller.safe_call_with_result(create_async_writer(), None)
        self._parent = _parent

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close(flush=False)

    def close(self, *, flush: bool = True, timeout: TimeoutType = None):
        if self._closed:
            return

        self._closed = True

        self._caller.safe_call_with_result(self._async_writer.close(flush=flush), timeout)

    def _check_closed(self):
        if self._closed:
            raise TopicWriterClosedError()

    def async_flush(self) -> Future:
        self._check_closed()

        return self._caller.unsafe_call_with_future(self._async_writer.flush())

    def flush(self, *, timeout=None):
        self._check_closed()

        return self._caller.unsafe_call_with_result(self._async_writer.flush(), timeout)

    def async_wait_init(self) -> Future[PublicWriterInitInfo]:
        self._check_closed()

        return self._caller.unsafe_call_with_future(self._async_writer.wait_init())

    def wait_init(self, *, timeout: TimeoutType = None) -> PublicWriterInitInfo:
        self._check_closed()

        return self._caller.unsafe_call_with_result(self._async_writer.wait_init(), timeout)

    def write(
        self,
        messages: Union[Message, List[Message]],
        timeout: TimeoutType = None,
    ):
        self._check_closed()

        self._caller.safe_call_with_result(self._async_writer.write(messages), timeout)

    def async_write_with_ack(
        self,
        messages: Union[Message, List[Message]],
    ) -> Future[Union[PublicWriteResult, List[PublicWriteResult]]]:
        self._check_closed()

        return self._caller.unsafe_call_with_future(self._async_writer.write_with_ack(messages))

    def write_with_ack(
        self,
        messages: Union[Message, List[Message]],
        timeout: Union[float, None] = None,
    ) -> Union[PublicWriteResult, List[PublicWriteResult]]:
        self._check_closed()

        return self._caller.unsafe_call_with_result(self._async_writer.write_with_ack(messages), timeout=timeout)
