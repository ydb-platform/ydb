from __future__ import annotations

import asyncio
import concurrent.futures
import gzip
import typing
from asyncio import Task
from collections import deque
from typing import Optional, Set, Dict, Union, Callable

import ydb
from .. import _apis, issues
from .._utilities import AtomicCounter
from ..aio import Driver
from ..issues import Error as YdbError, _process_response
from . import datatypes
from . import topic_reader
from .._grpc.grpcwrapper.common_utils import (
    IGrpcWrapperAsyncIO,
    SupportedDriverType,
    GrpcWrapperAsyncIO,
)
from .._grpc.grpcwrapper.ydb_topic import (
    StreamReadMessage,
    UpdateTokenRequest,
    UpdateTokenResponse,
    Codec,
)
from .._errors import check_retriable_error
import logging

logger = logging.getLogger(__name__)


class TopicReaderError(YdbError):
    pass


class PublicTopicReaderUnexpectedCodecError(YdbError):
    pass


class PublicTopicReaderPartitionExpiredError(TopicReaderError):
    """
    Commit message when partition read session are dropped.
    It is ok - the message/batch will not commit to server and will receive in other read session
    (with this or other reader).
    """

    def __init__(self, message: str = "Topic reader partition session is closed"):
        super().__init__(message)


class TopicReaderStreamClosedError(TopicReaderError):
    def __init__(self):
        super().__init__("Topic reader stream is closed")


class TopicReaderClosedError(TopicReaderError):
    def __init__(self):
        super().__init__("Topic reader is closed already")


class PublicAsyncIOReader:
    _loop: asyncio.AbstractEventLoop
    _closed: bool
    _reconnector: ReaderReconnector
    _parent: typing.Any  # need for prevent close parent client by GC

    def __init__(
        self,
        driver: Driver,
        settings: topic_reader.PublicReaderSettings,
        *,
        _parent=None,
    ):
        self._loop = asyncio.get_running_loop()
        self._closed = False
        self._reconnector = ReaderReconnector(driver, settings)
        self._parent = _parent

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __del__(self):
        if not self._closed:
            self._loop.create_task(self.close(flush=False), name="close reader")

    async def wait_message(self):
        """
        Wait at least one message from reader.
        """
        await self._reconnector.wait_message()

    async def receive_batch(
        self,
    ) -> typing.Union[datatypes.PublicBatch, None]:
        """
        Get one messages batch from reader.
        All messages in a batch from same partition.

        use asyncio.wait_for for wait with timeout.
        """
        await self._reconnector.wait_message()
        return self._reconnector.receive_batch_nowait()

    async def receive_message(self) -> typing.Optional[datatypes.PublicMessage]:
        """
        Block until receive new message

        use asyncio.wait_for for wait with timeout.
        """
        await self._reconnector.wait_message()
        return self._reconnector.receive_message_nowait()

    def commit(self, batch: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]):
        """
        Write commit message to a buffer.

        For the method no way check the commit result
        (for example if lost connection - commits will not re-send and committed messages will receive again).
        """
        try:
            self._reconnector.commit(batch)
        except PublicTopicReaderPartitionExpiredError:
            pass

    async def commit_with_ack(self, batch: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]):
        """
        write commit message to a buffer and wait ack from the server.

        use asyncio.wait_for for wait with timeout.

        may raise ydb.TopicReaderPartitionExpiredError, the error mean reader partition closed from server
        before receive commit ack. Message may be acked or not (if not - it will send in other read session,
        to this or other reader).
        """
        waiter = self._reconnector.commit(batch)
        await waiter.future

    async def close(self, flush: bool = True):
        if self._closed:
            raise TopicReaderClosedError()

        self._closed = True
        await self._reconnector.close(flush)


class ReaderReconnector:
    _static_reader_reconnector_counter = AtomicCounter()

    _id: int
    _settings: topic_reader.PublicReaderSettings
    _driver: Driver
    _background_tasks: Set[Task]

    _state_changed: asyncio.Event
    _stream_reader: Optional["ReaderStream"]
    _first_error: asyncio.Future[YdbError]

    def __init__(self, driver: Driver, settings: topic_reader.PublicReaderSettings):
        self._id = self._static_reader_reconnector_counter.inc_and_get()
        self._settings = settings
        self._driver = driver
        self._background_tasks = set()

        self._state_changed = asyncio.Event()
        self._stream_reader = None
        self._background_tasks.add(asyncio.create_task(self._connection_loop()))
        self._first_error = asyncio.get_running_loop().create_future()

    async def _connection_loop(self):
        attempt = 0
        while True:
            try:
                self._stream_reader = await ReaderStream.create(self._id, self._driver, self._settings)
                attempt = 0
                self._state_changed.set()
                await self._stream_reader.wait_error()
            except BaseException as err:
                retry_info = check_retriable_error(err, self._settings._retry_settings(), attempt)
                if not retry_info.is_retriable:
                    self._set_first_error(err)
                    return
                await asyncio.sleep(retry_info.sleep_timeout_seconds)

                attempt += 1
            finally:
                if self._stream_reader is not None:
                    # noinspection PyBroadException
                    try:
                        await self._stream_reader.close(flush=False)
                    except BaseException:
                        # supress any error on close stream reader
                        pass

    async def wait_message(self):
        while True:
            if self._first_error.done():
                raise self._first_error.result()

            if self._stream_reader:
                try:
                    await self._stream_reader.wait_messages()
                    return
                except YdbError:
                    pass  # handle errors in reconnection loop

            await self._state_changed.wait()
            self._state_changed.clear()

    def receive_batch_nowait(self):
        return self._stream_reader.receive_batch_nowait()

    def receive_message_nowait(self):
        return self._stream_reader.receive_message_nowait()

    def commit(self, batch: datatypes.ICommittable) -> datatypes.PartitionSession.CommitAckWaiter:
        return self._stream_reader.commit(batch)

    async def close(self, flush: bool):
        if self._stream_reader:
            await self._stream_reader.close(flush)
        for task in self._background_tasks:
            task.cancel()

        await asyncio.wait(self._background_tasks)

    async def flush(self):
        if self._stream_reader:
            await self._stream_reader.flush()

    def _set_first_error(self, err: issues.Error):
        try:
            self._first_error.set_result(err)
            self._state_changed.set()
        except asyncio.InvalidStateError:
            # skip if already has result
            pass


class ReaderStream:
    _static_id_counter = AtomicCounter()

    _loop: asyncio.AbstractEventLoop
    _id: int
    _reader_reconnector_id: int
    _session_id: str
    _stream: Optional[IGrpcWrapperAsyncIO]
    _started: bool
    _background_tasks: Set[asyncio.Task]
    _partition_sessions: Dict[int, datatypes.PartitionSession]
    _buffer_size_bytes: int  # use for init request, then for debug purposes only
    _decode_executor: concurrent.futures.Executor
    _decoders: Dict[int, typing.Callable[[bytes], bytes]]  # dict[codec_code] func(encoded_bytes)->decoded_bytes

    if typing.TYPE_CHECKING:
        _batches_to_decode: asyncio.Queue[datatypes.PublicBatch]
    else:
        _batches_to_decode: asyncio.Queue

    _state_changed: asyncio.Event
    _closed: bool
    _message_batches: typing.Deque[datatypes.PublicBatch]
    _first_error: asyncio.Future[YdbError]

    _update_token_interval: Union[int, float]
    _update_token_event: asyncio.Event
    _get_token_function: Callable[[], str]

    def __init__(
        self,
        reader_reconnector_id: int,
        settings: topic_reader.PublicReaderSettings,
        get_token_function: Optional[Callable[[], str]] = None,
    ):
        self._loop = asyncio.get_running_loop()
        self._id = ReaderStream._static_id_counter.inc_and_get()
        self._reader_reconnector_id = reader_reconnector_id
        self._session_id = "not initialized"
        self._stream = None
        self._started = False
        self._background_tasks = set()
        self._partition_sessions = dict()
        self._buffer_size_bytes = settings.buffer_size_bytes
        self._decode_executor = settings.decoder_executor

        self._decoders = {Codec.CODEC_GZIP: gzip.decompress}
        if settings.decoders:
            self._decoders.update(settings.decoders)

        self._state_changed = asyncio.Event()
        self._closed = False
        self._first_error = asyncio.get_running_loop().create_future()
        self._batches_to_decode = asyncio.Queue()
        self._message_batches = deque()

        self._update_token_interval = settings.update_token_interval
        self._get_token_function = get_token_function
        self._update_token_event = asyncio.Event()

    @staticmethod
    async def create(
        reader_reconnector_id: int,
        driver: SupportedDriverType,
        settings: topic_reader.PublicReaderSettings,
    ) -> "ReaderStream":
        stream = GrpcWrapperAsyncIO(StreamReadMessage.FromServer.from_proto)

        await stream.start(driver, _apis.TopicService.Stub, _apis.TopicService.StreamRead)

        creds = driver._credentials
        reader = ReaderStream(
            reader_reconnector_id,
            settings,
            get_token_function=creds.get_auth_token if creds else None,
        )
        await reader._start(stream, settings._init_message())
        return reader

    async def _start(self, stream: IGrpcWrapperAsyncIO, init_message: StreamReadMessage.InitRequest):
        if self._started:
            raise TopicReaderError("Double start ReaderStream")

        self._started = True
        self._stream = stream

        stream.write(StreamReadMessage.FromClient(client_message=init_message))
        init_response = await stream.receive()  # type: StreamReadMessage.FromServer
        if isinstance(init_response.server_message, StreamReadMessage.InitResponse):
            self._session_id = init_response.server_message.session_id
        else:
            raise TopicReaderError("Unexpected message after InitRequest: %s", init_response)

        self._update_token_event.set()

        self._background_tasks.add(asyncio.create_task(self._read_messages_loop(), name="read_messages_loop"))
        self._background_tasks.add(asyncio.create_task(self._decode_batches_loop(), name="decode_batches"))
        if self._get_token_function:
            self._background_tasks.add(asyncio.create_task(self._update_token_loop(), name="update_token_loop"))
        self._background_tasks.add(
            asyncio.create_task(self._handle_background_errors(), name="handle_background_errors")
        )

    async def wait_error(self):
        raise await self._first_error

    async def wait_messages(self):
        while True:
            if self._get_first_error():
                raise self._get_first_error()

            if self._message_batches:
                return

            await self._state_changed.wait()
            self._state_changed.clear()

    def receive_batch_nowait(self):
        if self._get_first_error():
            raise self._get_first_error()

        if not self._message_batches:
            return None

        batch = self._message_batches.popleft()
        self._buffer_release_bytes(batch._bytes_size)
        return batch

    def receive_message_nowait(self):
        if self._get_first_error():
            raise self._get_first_error()

        try:
            batch = self._message_batches[0]
            message = batch.pop_message()
        except IndexError:
            return None

        if batch.empty():
            self.receive_batch_nowait()

        return message

    def commit(self, batch: datatypes.ICommittable) -> datatypes.PartitionSession.CommitAckWaiter:
        partition_session = batch._commit_get_partition_session()

        if partition_session.reader_reconnector_id != self._reader_reconnector_id:
            raise TopicReaderError("reader can commit only self-produced messages")

        if partition_session.reader_stream_id != self._id:
            raise PublicTopicReaderPartitionExpiredError("commit messages after reconnect to server")

        if partition_session.id not in self._partition_sessions:
            raise PublicTopicReaderPartitionExpiredError("commit messages after server stop the partition read session")

        commit_range = batch._commit_get_offsets_range()
        waiter = partition_session.add_waiter(commit_range.end)

        if not waiter.future.done():
            client_message = StreamReadMessage.CommitOffsetRequest(
                commit_offsets=[
                    StreamReadMessage.CommitOffsetRequest.PartitionCommitOffset(
                        partition_session_id=partition_session.id,
                        offsets=[commit_range],
                    )
                ]
            )
            self._stream.write(StreamReadMessage.FromClient(client_message=client_message))

        return waiter

    async def _handle_background_errors(self):
        done, _ = await asyncio.wait(self._background_tasks, return_when=asyncio.FIRST_EXCEPTION)
        for f in done:
            f = f  # type: asyncio.Future
            err = f.exception()
            if not isinstance(err, ydb.Error):
                old_err = err
                err = ydb.Error("Background process failed unexpected")
                err.__cause__ = old_err
            self._set_first_error(err)

    async def _read_messages_loop(self):
        try:
            self._stream.write(
                StreamReadMessage.FromClient(
                    client_message=StreamReadMessage.ReadRequest(
                        bytes_size=self._buffer_size_bytes,
                    ),
                )
            )
            while True:
                try:
                    message = await self._stream.receive()  # type: StreamReadMessage.FromServer
                    _process_response(message.server_status)

                    if isinstance(message.server_message, StreamReadMessage.ReadResponse):
                        self._on_read_response(message.server_message)

                    elif isinstance(message.server_message, StreamReadMessage.CommitOffsetResponse):
                        self._on_commit_response(message.server_message)

                    elif isinstance(
                        message.server_message,
                        StreamReadMessage.StartPartitionSessionRequest,
                    ):
                        self._on_start_partition_session(message.server_message)

                    elif isinstance(
                        message.server_message,
                        StreamReadMessage.StopPartitionSessionRequest,
                    ):
                        self._on_partition_session_stop(message.server_message)

                    elif isinstance(message.server_message, UpdateTokenResponse):
                        self._update_token_event.set()

                    else:
                        raise issues.UnexpectedGrpcMessage(
                            "Unexpected message in _read_messages_loop: %s" % type(message.server_message)
                        )
                except issues.UnexpectedGrpcMessage as e:
                    logger.exception("unexpected message in stream reader: %s" % e)

                self._state_changed.set()
        except Exception as e:
            self._set_first_error(e)
            return

    async def _update_token_loop(self):
        while True:
            await asyncio.sleep(self._update_token_interval)
            await self._update_token(token=self._get_token_function())

    async def _update_token(self, token: str):
        await self._update_token_event.wait()
        try:
            msg = StreamReadMessage.FromClient(UpdateTokenRequest(token))
            self._stream.write(msg)
        finally:
            self._update_token_event.clear()

    def _on_start_partition_session(self, message: StreamReadMessage.StartPartitionSessionRequest):
        try:
            if message.partition_session.partition_session_id in self._partition_sessions:
                raise TopicReaderError(
                    "Double start partition session: %s" % message.partition_session.partition_session_id
                )

            self._partition_sessions[message.partition_session.partition_session_id] = datatypes.PartitionSession(
                id=message.partition_session.partition_session_id,
                state=datatypes.PartitionSession.State.Active,
                topic_path=message.partition_session.path,
                partition_id=message.partition_session.partition_id,
                committed_offset=message.committed_offset,
                reader_reconnector_id=self._reader_reconnector_id,
                reader_stream_id=self._id,
            )
            self._stream.write(
                StreamReadMessage.FromClient(
                    client_message=StreamReadMessage.StartPartitionSessionResponse(
                        partition_session_id=message.partition_session.partition_session_id,
                        read_offset=None,
                        commit_offset=None,
                    )
                ),
            )
        except YdbError as err:
            self._set_first_error(err)

    def _on_partition_session_stop(self, message: StreamReadMessage.StopPartitionSessionRequest):
        if message.partition_session_id not in self._partition_sessions:
            # may if receive stop partition with graceful=false after response on stop partition
            # with graceful=true and remove partition from internal dictionary
            return

        partition = self._partition_sessions.pop(message.partition_session_id)
        partition.close()

        if message.graceful:
            self._stream.write(
                StreamReadMessage.FromClient(
                    client_message=StreamReadMessage.StopPartitionSessionResponse(
                        partition_session_id=message.partition_session_id,
                    )
                )
            )

    def _on_read_response(self, message: StreamReadMessage.ReadResponse):
        self._buffer_consume_bytes(message.bytes_size)

        batches = self._read_response_to_batches(message)
        for batch in batches:
            self._batches_to_decode.put_nowait(batch)

    def _on_commit_response(self, message: StreamReadMessage.CommitOffsetResponse):
        for partition_offset in message.partitions_committed_offsets:
            if partition_offset.partition_session_id not in self._partition_sessions:
                continue

            session = self._partition_sessions[partition_offset.partition_session_id]
            session.ack_notify(partition_offset.committed_offset)

    def _buffer_consume_bytes(self, bytes_size):
        self._buffer_size_bytes -= bytes_size

    def _buffer_release_bytes(self, bytes_size):
        self._buffer_size_bytes += bytes_size
        self._stream.write(
            StreamReadMessage.FromClient(
                client_message=StreamReadMessage.ReadRequest(
                    bytes_size=bytes_size,
                )
            )
        )

    def _read_response_to_batches(self, message: StreamReadMessage.ReadResponse) -> typing.List[datatypes.PublicBatch]:
        batches = []

        batch_count = sum(len(p.batches) for p in message.partition_data)
        if batch_count == 0:
            return batches

        bytes_per_batch = message.bytes_size // batch_count
        additional_bytes_to_last_batch = message.bytes_size - bytes_per_batch * batch_count

        for partition_data in message.partition_data:
            partition_session = self._partition_sessions[partition_data.partition_session_id]
            for server_batch in partition_data.batches:
                messages = []
                for message_data in server_batch.message_data:
                    mess = datatypes.PublicMessage(
                        seqno=message_data.seq_no,
                        created_at=message_data.created_at,
                        message_group_id=message_data.message_group_id,
                        session_metadata=server_batch.write_session_meta,
                        offset=message_data.offset,
                        written_at=server_batch.written_at,
                        producer_id=server_batch.producer_id,
                        data=message_data.data,
                        _partition_session=partition_session,
                        _commit_start_offset=partition_session._next_message_start_commit_offset,
                        _commit_end_offset=message_data.offset + 1,
                    )
                    messages.append(mess)
                    partition_session._next_message_start_commit_offset = mess._commit_end_offset

                if messages:
                    batch = datatypes.PublicBatch(
                        messages=messages,
                        _partition_session=partition_session,
                        _bytes_size=bytes_per_batch,
                        _codec=Codec(server_batch.codec),
                    )
                    batches.append(batch)

        batches[-1]._bytes_size += additional_bytes_to_last_batch
        return batches

    async def _decode_batches_loop(self):
        while True:
            batch = await self._batches_to_decode.get()
            await self._decode_batch_inplace(batch)
            self._message_batches.append(batch)
            self._state_changed.set()

    async def _decode_batch_inplace(self, batch):
        if batch._codec == Codec.CODEC_RAW:
            return

        try:
            decode_func = self._decoders[batch._codec]
        except KeyError:
            raise PublicTopicReaderUnexpectedCodecError("Receive message with unexpected codec: %s" % batch._codec)

        decode_data_futures = []
        for message in batch.messages:
            future = self._loop.run_in_executor(self._decode_executor, decode_func, message.data)
            decode_data_futures.append(future)

        decoded_data = await asyncio.gather(*decode_data_futures)
        for index, message in enumerate(batch.messages):
            message.data = decoded_data[index]

        batch._codec = Codec.CODEC_RAW

    def _set_first_error(self, err: YdbError):
        try:
            self._first_error.set_result(err)
            self._state_changed.set()
        except asyncio.InvalidStateError:
            # skip later set errors
            pass

    def _get_first_error(self) -> Optional[YdbError]:
        if self._first_error.done():
            return self._first_error.result()

    async def flush(self):
        futures = []
        for session in self._partition_sessions.values():
            futures.extend(w.future for w in session._ack_waiters)

        if futures:
            await asyncio.wait(futures)

    async def close(self, flush: bool):
        if self._closed:
            return

        self._closed = True

        if flush:
            await self.flush()

        self._set_first_error(TopicReaderStreamClosedError())
        self._state_changed.set()
        self._stream.close()

        for session in self._partition_sessions.values():
            session.close()
        self._partition_sessions.clear()

        for task in self._background_tasks:
            task.cancel()

        if self._background_tasks:
            await asyncio.wait(self._background_tasks)
