import asyncio
import concurrent.futures
import datetime
import functools
import gzip
import typing
from collections import deque
from typing import Deque, AsyncIterator, Union, List, Optional, Dict, Callable

import logging

import ydb
from .topic_writer import (
    PublicWriterSettings,
    WriterSettings,
    PublicMessage,
    PublicWriterInitInfo,
    InternalMessage,
    TopicWriterStopped,
    TopicWriterError,
    messages_to_proto_requests,
    PublicWriteResult,
    PublicWriteResultTypes,
    Message,
)
from .. import (
    _apis,
    issues,
    check_retriable_error,
    RetrySettings,
)
from .._grpc.grpcwrapper.ydb_topic_public_types import PublicCodec
from .._grpc.grpcwrapper.ydb_topic import (
    UpdateTokenRequest,
    UpdateTokenResponse,
    StreamWriteMessage,
    WriterMessagesFromServerToClient,
)
from .._grpc.grpcwrapper.common_utils import (
    IGrpcWrapperAsyncIO,
    SupportedDriverType,
    GrpcWrapperAsyncIO,
)

logger = logging.getLogger(__name__)


class WriterAsyncIO:
    _loop: asyncio.AbstractEventLoop
    _reconnector: "WriterAsyncIOReconnector"
    _closed: bool
    _parent: typing.Any  # need for prevent close parent client by GC

    def __init__(
        self,
        driver: SupportedDriverType,
        settings: PublicWriterSettings,
        _client=None,
    ):
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._reconnector = WriterAsyncIOReconnector(driver=driver, settings=WriterSettings(settings))
        self._parent = _client

    async def __aenter__(self) -> "WriterAsyncIO":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.close()
        except BaseException:
            if exc_val is None:
                raise

    def __del__(self):
        if self._closed or self._loop.is_closed():
            return

        self._loop.call_soon(functools.partial(self.close, flush=False))

    async def close(self, *, flush: bool = True):
        if self._closed:
            return

        self._closed = True

        await self._reconnector.close(flush)

    async def write_with_ack(
        self,
        messages: Union[Message, List[Message]],
    ) -> Union[PublicWriteResultTypes, List[PublicWriteResultTypes]]:
        """
        IT IS SLOWLY WAY. IT IS BAD CHOISE IN MOST CASES.
        It is recommended to use write with optionally flush or write_with_ack_futures and receive acks by wait futures.

        send one or number of messages to server and wait acks.

        For wait with timeout use asyncio.wait_for.
        """
        futures = await self.write_with_ack_future(messages)
        if not isinstance(futures, list):
            futures = [futures]

        await asyncio.wait(futures)
        results = [f.result() for f in futures]

        return results if isinstance(messages, list) else results[0]

    async def write_with_ack_future(
        self,
        messages: Union[Message, List[Message]],
    ) -> Union[asyncio.Future, List[asyncio.Future]]:
        """
        send one or number of messages to server.
        return feature, which can be waited for check send result.

        Usually it is fast method, but can wait if internal buffer is full.

        For wait with timeout use asyncio.wait_for.
        """
        input_single_message = not isinstance(messages, list)
        converted_messages = []
        if isinstance(messages, list):
            for m in messages:
                converted_messages.append(PublicMessage._create_message(m))
        else:
            converted_messages = [PublicMessage._create_message(messages)]

        futures = await self._reconnector.write_with_ack_future(converted_messages)
        if input_single_message:
            return futures[0]
        else:
            return futures

    async def write(
        self,
        messages: Union[Message, List[Message]],
    ):
        """
        send one or number of messages to server.
        it put message to internal buffer

        For wait with timeout use asyncio.wait_for.
        """
        await self.write_with_ack_future(messages)

    async def flush(self):
        """
        Force send all messages from internal buffer and wait acks from server for all
        messages.

        For wait with timeout use asyncio.wait_for.
        """
        return await self._reconnector.flush()

    async def wait_init(self) -> PublicWriterInitInfo:
        """
        wait while real connection will be established to server.

        For wait with timeout use asyncio.wait_for()
        """
        return await self._reconnector.wait_init()


class WriterAsyncIOReconnector:
    _closed: bool
    _loop: asyncio.AbstractEventLoop
    _credentials: Union[ydb.credentials.Credentials, None]
    _driver: ydb.aio.Driver
    _init_message: StreamWriteMessage.InitRequest
    _stream_connected: asyncio.Event
    _settings: WriterSettings
    _codec: PublicCodec
    _codec_functions: Dict[PublicCodec, Callable[[bytes], bytes]]
    _encode_executor: Optional[concurrent.futures.Executor]
    _codec_selector_batch_num: int
    _codec_selector_last_codec: Optional[PublicCodec]
    _codec_selector_check_batches_interval: int

    if typing.TYPE_CHECKING:
        _messages_for_encode: asyncio.Queue[List[InternalMessage]]
    else:
        _messages_for_encode: asyncio.Queue
    _messages: Deque[InternalMessage]
    _messages_future: Deque[asyncio.Future]
    _new_messages: asyncio.Queue
    _background_tasks: List[asyncio.Task]

    _state_changed: asyncio.Event
    if typing.TYPE_CHECKING:
        _stop_reason: asyncio.Future[BaseException]
    else:
        _stop_reason: asyncio.Future
    _init_info: Optional[PublicWriterInitInfo]

    def __init__(self, driver: SupportedDriverType, settings: WriterSettings):
        self._closed = False
        self._loop = asyncio.get_running_loop()
        self._driver = driver
        self._credentials = driver._credentials
        self._init_message = settings.create_init_request()
        self._new_messages = asyncio.Queue()
        self._init_info = None
        self._stream_connected = asyncio.Event()
        self._settings = settings

        self._codec_functions = {
            PublicCodec.RAW: lambda data: data,
            PublicCodec.GZIP: gzip.compress,
        }

        if settings.encoders:
            self._codec_functions.update(settings.encoders)

        self._encode_executor = settings.encoder_executor

        self._codec_selector_batch_num = 0
        self._codec_selector_last_codec = None
        self._codec_selector_check_batches_interval = 10000

        self._codec = self._settings.codec
        if self._codec and self._codec not in self._codec_functions:
            known_codecs = sorted(self._codec_functions.keys())
            raise ValueError("Unknown codec for writer: %s, supported codecs: %s" % (self._codec, known_codecs))

        self._last_known_seq_no = 0
        self._messages_for_encode = asyncio.Queue()
        self._messages = deque()
        self._messages_future = deque()
        self._new_messages = asyncio.Queue()
        self._stop_reason = self._loop.create_future()
        self._background_tasks = [
            asyncio.create_task(self._connection_loop(), name="connection_loop"),
            asyncio.create_task(self._encode_loop(), name="encode_loop"),
        ]

        self._state_changed = asyncio.Event()

    async def close(self, flush: bool):
        if self._closed:
            return
        self._closed = True
        logger.debug("Close writer reconnector")

        if flush:
            await self.flush()

        self._stop(TopicWriterStopped())

        for task in self._background_tasks:
            task.cancel()
        await asyncio.wait(self._background_tasks)

        # if work was stopped before close by error - raise the error
        try:
            self._check_stop()
        except TopicWriterStopped:
            pass

    async def wait_init(self) -> PublicWriterInitInfo:
        while True:
            if self._stop_reason.done():
                raise self._stop_reason.exception()

            if self._init_info:
                return self._init_info

            await self._state_changed.wait()

    async def wait_stop(self) -> BaseException:
        try:
            await self._stop_reason
        except BaseException as stop_reason:
            return stop_reason

    async def write_with_ack_future(self, messages: List[PublicMessage]) -> List[asyncio.Future]:
        # todo check internal buffer limit
        self._check_stop()

        if self._settings.auto_seqno:
            await self.wait_init()

        internal_messages = self._prepare_internal_messages(messages)
        messages_future = [self._loop.create_future() for _ in internal_messages]

        self._messages_future.extend(messages_future)

        if self._codec == PublicCodec.RAW:
            self._add_messages_to_send_queue(internal_messages)
        else:
            self._messages_for_encode.put_nowait(internal_messages)

        return messages_future

    def _add_messages_to_send_queue(self, internal_messages: List[InternalMessage]):
        self._messages.extend(internal_messages)
        for m in internal_messages:
            self._new_messages.put_nowait(m)

    def _prepare_internal_messages(self, messages: List[PublicMessage]) -> List[InternalMessage]:
        if self._settings.auto_created_at:
            now = datetime.datetime.now()
        else:
            now = None

        res = []
        for m in messages:
            internal_message = InternalMessage(m)
            if self._settings.auto_seqno:
                if internal_message.seq_no is None:
                    self._last_known_seq_no += 1
                    internal_message.seq_no = self._last_known_seq_no
                else:
                    raise TopicWriterError("Explicit seqno and auto_seq setting is mutual exclusive")
            else:
                if internal_message.seq_no is None or internal_message.seq_no == 0:
                    raise TopicWriterError("Empty seqno and auto_seq setting is disabled")
                elif internal_message.seq_no <= self._last_known_seq_no:
                    raise TopicWriterError("Message seqno is duplicated: %s" % internal_message.seq_no)
                else:
                    self._last_known_seq_no = internal_message.seq_no

            if self._settings.auto_created_at:
                if internal_message.created_at is not None:
                    raise TopicWriterError(
                        "Explicit set auto_created_at and setting auto_created_at is mutual exclusive"
                    )
                else:
                    internal_message.created_at = now

            res.append(internal_message)

        return res

    def _check_stop(self):
        if self._stop_reason.done():
            raise self._stop_reason.exception()

    async def _connection_loop(self):
        retry_settings = RetrySettings()  # todo

        while True:
            attempt = 0  # todo calc and reset
            tasks = []

            # noinspection PyBroadException
            stream_writer = None
            try:
                stream_writer = await WriterAsyncIOStream.create(
                    self._driver,
                    self._init_message,
                    self._settings.update_token_interval,
                )
                try:
                    if self._init_info is None:
                        self._last_known_seq_no = stream_writer.last_seqno
                        self._init_info = PublicWriterInitInfo(
                            last_seqno=stream_writer.last_seqno,
                            supported_codecs=stream_writer.supported_codecs,
                        )
                        self._state_changed.set()

                except asyncio.InvalidStateError:
                    pass

                self._stream_connected.set()

                send_loop = asyncio.create_task(self._send_loop(stream_writer), name="writer send loop")
                receive_loop = asyncio.create_task(self._read_loop(stream_writer), name="writer receive loop")

                tasks = [send_loop, receive_loop]
                done, _ = await asyncio.wait([send_loop, receive_loop], return_when=asyncio.FIRST_COMPLETED)
                done.pop().result()  # need for raise exception - reason of stop task
            except issues.Error as err:
                err_info = check_retriable_error(err, retry_settings, attempt)
                if not err_info.is_retriable:
                    self._stop(err)
                    return

                await asyncio.sleep(err_info.sleep_timeout_seconds)

            except (asyncio.CancelledError, Exception) as err:
                self._stop(err)
                return
            finally:
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.wait(tasks)
                if stream_writer:
                    await stream_writer.close()

    async def _encode_loop(self):
        try:
            while True:
                messages = await self._messages_for_encode.get()
                while not self._messages_for_encode.empty():
                    messages.extend(self._messages_for_encode.get_nowait())

                batch_codec = await self._codec_selector(messages)
                await self._encode_data_inplace(batch_codec, messages)
                self._add_messages_to_send_queue(messages)
        except BaseException as err:
            self._stop(err)

    async def _encode_data_inplace(self, codec: PublicCodec, messages: List[InternalMessage]):
        if codec == PublicCodec.RAW:
            return

        eventloop = asyncio.get_running_loop()
        encode_waiters = []
        encoder_function = self._codec_functions[codec]

        for message in messages:
            encoded_data_futures = eventloop.run_in_executor(
                self._encode_executor, encoder_function, message.get_bytes()
            )
            encode_waiters.append(encoded_data_futures)

        encoded_datas = await asyncio.gather(*encode_waiters)

        for index, data in enumerate(encoded_datas):
            message = messages[index]
            message.codec = codec
            message.data = data

    async def _codec_selector(self, messages: List[InternalMessage]) -> PublicCodec:
        if self._codec is not None:
            return self._codec

        if self._codec_selector_last_codec is None:
            available_codecs = await self._get_available_codecs()

            # use every of available encoders at start for prevent problems
            # with rare used encoders (on writer or reader side)
            if self._codec_selector_batch_num < len(available_codecs):
                codec = available_codecs[self._codec_selector_batch_num]
            else:
                codec = await self._codec_selector_by_check_compress(messages)
                self._codec_selector_last_codec = codec
        else:
            if self._codec_selector_batch_num % self._codec_selector_check_batches_interval == 0:
                self._codec_selector_last_codec = await self._codec_selector_by_check_compress(messages)
            codec = self._codec_selector_last_codec
        self._codec_selector_batch_num += 1
        return codec

    async def _get_available_codecs(self) -> List[PublicCodec]:
        info = await self.wait_init()
        topic_supported_codecs = info.supported_codecs
        if not topic_supported_codecs:
            topic_supported_codecs = [PublicCodec.RAW, PublicCodec.GZIP]

        res = []
        for codec in topic_supported_codecs:
            if codec in self._codec_functions:
                res.append(codec)

        if not res:
            raise TopicWriterError("Writer does not support topic's codecs")

        res.sort()

        return res

    async def _codec_selector_by_check_compress(self, messages: List[InternalMessage]) -> PublicCodec:
        """
        Try to compress messages and choose codec with the smallest result size.
        """

        test_messages = messages[:10]

        available_codecs = await self._get_available_codecs()
        if len(available_codecs) == 1:
            return available_codecs[0]

        def get_compressed_size(codec) -> int:
            s = 0
            f = self._codec_functions[codec]

            for m in test_messages:
                encoded = f(m.get_bytes())
                s += len(encoded)

            return s

        def select_codec() -> PublicCodec:
            min_codec = available_codecs[0]
            min_size = get_compressed_size(min_codec)
            for codec in available_codecs[1:]:
                size = get_compressed_size(codec)
                if size < min_size:
                    min_codec = codec
                    min_size = size
            return min_codec

        loop = asyncio.get_running_loop()
        codec = await loop.run_in_executor(self._encode_executor, select_codec)
        return codec

    async def _read_loop(self, writer: "WriterAsyncIOStream"):
        while True:
            resp = await writer.receive()

            for ack in resp.acks:
                self._handle_receive_ack(ack)

    def _handle_receive_ack(self, ack):
        current_message = self._messages.popleft()
        message_future = self._messages_future.popleft()
        if current_message.seq_no != ack.seq_no:
            raise TopicWriterError(
                "internal error - receive unexpected ack. Expected seqno: %s, received seqno: %s"
                % (current_message.seq_no, ack.seq_no)
            )
        write_ack_msg = StreamWriteMessage.WriteResponse.WriteAck
        status = ack.message_write_status
        if isinstance(status, write_ack_msg.StatusSkipped):
            result = PublicWriteResult.Skipped()
        elif isinstance(status, write_ack_msg.StatusWritten):
            result = PublicWriteResult.Written(offset=status.offset)
        else:
            raise TopicWriterError("internal error - receive unexpected ack message.")
        message_future.set_result(result)

    async def _send_loop(self, writer: "WriterAsyncIOStream"):
        try:
            messages = list(self._messages)

            last_seq_no = 0
            for m in messages:
                writer.write([m])
                last_seq_no = m.seq_no

            while True:
                m = await self._new_messages.get()  # type: InternalMessage
                if m.seq_no > last_seq_no:
                    writer.write([m])
        except asyncio.CancelledError:
            # the loop task cancelled be parent code, for example for reconnection
            # no need to stop all work.
            raise
        except BaseException as e:
            self._stop(e)
            raise

    def _stop(self, reason: BaseException):
        if reason is None:
            raise Exception("writer stop reason can not be None")

        if self._stop_reason.done():
            return

        self._stop_reason.set_exception(reason)

        for f in self._messages_future:
            f.set_exception(reason)

        self._state_changed.set()
        logger.info("Stop topic writer: %s" % reason)

    async def flush(self):
        if not self._messages_future:
            return

        # wait last message
        await asyncio.wait(self._messages_future)


class WriterAsyncIOStream:
    # todo slots
    _closed: bool

    last_seqno: int
    supported_codecs: Optional[List[PublicCodec]]

    _stream: IGrpcWrapperAsyncIO
    _requests: asyncio.Queue
    _responses: AsyncIterator

    _update_token_interval: Optional[Union[int, float]]
    _update_token_task: Optional[asyncio.Task]
    _update_token_event: asyncio.Event
    _get_token_function: Optional[Callable[[], str]]

    def __init__(
        self,
        update_token_interval: Optional[Union[int, float]] = None,
        get_token_function: Optional[Callable[[], str]] = None,
    ):
        self._closed = False

        self._update_token_interval = update_token_interval
        self._get_token_function = get_token_function
        self._update_token_event = asyncio.Event()
        self._update_token_task = None

    async def close(self):
        if self._closed:
            return
        self._closed = True

        if self._update_token_task:
            self._update_token_task.cancel()
            await asyncio.wait([self._update_token_task])

        self._stream.close()

    @staticmethod
    async def create(
        driver: SupportedDriverType,
        init_request: StreamWriteMessage.InitRequest,
        update_token_interval: Optional[Union[int, float]] = None,
    ) -> "WriterAsyncIOStream":
        stream = GrpcWrapperAsyncIO(StreamWriteMessage.FromServer.from_proto)

        await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamWrite)

        creds = driver._credentials
        writer = WriterAsyncIOStream(
            update_token_interval=update_token_interval,
            get_token_function=creds.get_auth_token if creds else lambda: "",
        )
        await writer._start(stream, init_request)
        return writer

    async def receive(self) -> StreamWriteMessage.WriteResponse:
        while True:
            item = await self._stream.receive()

            if isinstance(item, StreamWriteMessage.WriteResponse):
                return item
            if isinstance(item, UpdateTokenResponse):
                self._update_token_event.set()
                continue

            # todo log unknown messages instead of raise exception
            raise Exception("Unknown message while read writer answers: %s" % item)

    async def _start(self, stream: IGrpcWrapperAsyncIO, init_message: StreamWriteMessage.InitRequest):
        stream.write(StreamWriteMessage.FromClient(init_message))

        resp = await stream.receive()
        self._ensure_ok(resp)
        if not isinstance(resp, StreamWriteMessage.InitResponse):
            raise TopicWriterError("Unexpected answer for init request: %s" % resp)

        self.last_seqno = resp.last_seq_no
        self.supported_codecs = [PublicCodec(codec) for codec in resp.supported_codecs]

        self._stream = stream

        if self._update_token_interval is not None:
            self._update_token_event.set()
            self._update_token_task = asyncio.create_task(self._update_token_loop(), name="update_token_loop")

    @staticmethod
    def _ensure_ok(message: WriterMessagesFromServerToClient):
        if not message.status.is_success():
            raise TopicWriterError(f"status error from server in writer: {message.status}")

    def write(self, messages: List[InternalMessage]):
        if self._closed:
            raise RuntimeError("Can not write on closed stream.")

        for request in messages_to_proto_requests(messages):
            self._stream.write(request)

    async def _update_token_loop(self):
        while True:
            await asyncio.sleep(self._update_token_interval)
            await self._update_token(token=self._get_token_function())

    async def _update_token(self, token: str):
        await self._update_token_event.wait()
        try:
            msg = StreamWriteMessage.FromClient(UpdateTokenRequest(token))
            self._stream.write(msg)
        finally:
            self._update_token_event.clear()
