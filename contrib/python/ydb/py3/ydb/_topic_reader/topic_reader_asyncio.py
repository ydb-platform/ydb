from __future__ import annotations

import asyncio
import concurrent.futures
import gzip
import typing
from asyncio import Task
from collections import defaultdict, OrderedDict
from typing import Optional, Set, Dict, Union, Callable

import ydb
from .. import _apis, issues
from .._topic_common import common as topic_common
from .._utilities import AtomicCounter
from ..aio import Driver
from ..issues import Error as YdbError, _process_response
from . import datatypes
from . import events
from . import topic_reader
from .._grpc.grpcwrapper.common_utils import (
    IGrpcWrapperAsyncIO,
    SupportedDriverType,
    to_thread,
    GrpcWrapperAsyncIO,
)
from .._grpc.grpcwrapper.ydb_topic import (
    StreamReadMessage,
    UpdateTokenRequest,
    UpdateTokenResponse,
    UpdateOffsetsInTransactionRequest,
    Codec,
)
from .._errors import check_retriable_error
import logging

from ..query.base import TxEvent

if typing.TYPE_CHECKING:
    from ..query.transaction import BaseQueryTxContext

from .._constants import DEFAULT_INITIAL_RESPONSE_TIMEOUT

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
    _settings: topic_reader.PublicReaderSettings
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
        self._settings = settings
        self._reconnector = ReaderReconnector(driver, settings, self._loop)
        self._parent = _parent

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __del__(self):
        if not self._closed:
            try:
                logger.debug("Topic reader was not closed properly. Consider using method close().")
                task = self._loop.create_task(self.close(flush=False))
                topic_common.wrap_set_name_for_asyncio_task(task, task_name="close reader")
            except BaseException:
                logger.warning("Something went wrong during reader close in __del__")

    async def wait_message(self):
        """
        Wait at least one message from reader.
        """
        await self._reconnector.wait_message()

    async def receive_batch(
        self,
        max_messages: typing.Union[int, None] = None,
    ) -> typing.Union[datatypes.PublicBatch, None]:
        """
        Get one messages batch from reader.
        All messages in a batch from same partition.

        use asyncio.wait_for for wait with timeout.
        """
        logger.debug("receive_batch max_messages=%s", max_messages)
        await self._reconnector.wait_message()
        return self._reconnector.receive_batch_nowait(
            max_messages=max_messages,
        )

    async def receive_batch_with_tx(
        self,
        tx: "BaseQueryTxContext",
        max_messages: typing.Union[int, None] = None,
    ) -> typing.Union[datatypes.PublicBatch, None]:
        """
        Get one messages batch with tx from reader.
        All messages in a batch from same partition.

        use asyncio.wait_for for wait with timeout.
        """
        logger.debug("receive_batch_with_tx tx=%s max_messages=%s", tx, max_messages)
        await self._reconnector.wait_message()
        return self._reconnector.receive_batch_with_tx_nowait(
            tx=tx,
            max_messages=max_messages,
        )

    async def receive_message(self) -> typing.Optional[datatypes.PublicMessage]:
        """
        Block until receive new message

        use asyncio.wait_for for wait with timeout.
        """
        logger.debug("receive_message")
        await self._reconnector.wait_message()
        return self._reconnector.receive_message_nowait()

    def commit(self, batch: typing.Union[datatypes.PublicMessage, datatypes.PublicBatch]):
        """
        Write commit message to a buffer.

        For the method no way check the commit result
        (for example if lost connection - commits will not re-send and committed messages will receive again).
        """
        logger.debug("commit message or batch")
        if self._settings.consumer is None:
            raise issues.Error("Commit operations are not supported for topic reader without consumer.")

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
        logger.debug("commit_with_ack message or batch")
        if self._settings.consumer is None:
            raise issues.Error("Commit operations are not supported for topic reader without consumer.")

        waiter = self._reconnector.commit(batch)
        await waiter.future

    async def close(self, flush: bool = True):
        if self._closed:
            raise TopicReaderClosedError()

        logger.debug("Close topic reader")
        self._closed = True
        await self._reconnector.close(flush)
        logger.debug("Topic reader was closed")

    @property
    def read_session_id(self) -> Optional[str]:
        return self._reconnector.read_session_id


class ReaderReconnector:
    _static_reader_reconnector_counter = AtomicCounter()

    _id: int
    _settings: topic_reader.PublicReaderSettings
    _driver: Driver
    _background_tasks: Set[Task]

    _state_changed: asyncio.Event
    _stream_reader: Optional["ReaderStream"]
    _first_error: asyncio.Future[YdbError]
    _tx_to_batches_map: Dict[str, typing.List[datatypes.PublicBatch]]

    def __init__(
        self,
        driver: Driver,
        settings: topic_reader.PublicReaderSettings,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._id = ReaderReconnector._static_reader_reconnector_counter.inc_and_get()
        self._settings = settings
        self._driver = driver
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._background_tasks = set()
        logger.debug("init reader reconnector id=%s", self._id)

        self._state_changed = asyncio.Event()
        self._stream_reader = None
        self._background_tasks.add(asyncio.create_task(self._connection_loop()))
        self._first_error = asyncio.get_running_loop().create_future()

        self._tx_to_batches_map = dict()

    async def _connection_loop(self):
        attempt = 0
        while True:
            try:
                logger.debug("reader %s connect attempt %s", self._id, attempt)
                self._stream_reader = await ReaderStream.create(self._id, self._driver, self._settings)
                logger.debug("reader %s connected stream %s", self._id, self._stream_reader._id)
                attempt = 0
                self._state_changed.set()
                await self._stream_reader.wait_error()
            except BaseException as err:
                retry_info = check_retriable_error(err, self._settings._retry_settings(), attempt)
                if not retry_info.is_retriable:
                    logger.debug("reader %s stop connection loop due to %s", self._id, err)
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

    def receive_batch_nowait(self, max_messages: Optional[int] = None):
        return self._stream_reader.receive_batch_nowait(
            max_messages=max_messages,
        )

    def receive_batch_with_tx_nowait(self, tx: "BaseQueryTxContext", max_messages: Optional[int] = None):
        batch = self._stream_reader.receive_batch_nowait(
            max_messages=max_messages,
        )

        self._init_tx(tx)

        self._tx_to_batches_map[tx.tx_id].append(batch)

        tx._add_callback(TxEvent.AFTER_COMMIT, batch._update_partition_offsets, self._loop)

        return batch

    def receive_message_nowait(self):
        return self._stream_reader.receive_message_nowait()

    def _init_tx(self, tx: "BaseQueryTxContext"):
        if tx.tx_id not in self._tx_to_batches_map:  # Init tx callbacks
            self._tx_to_batches_map[tx.tx_id] = []
            tx._add_callback(TxEvent.BEFORE_COMMIT, self._commit_batches_with_tx, self._loop)
            tx._add_callback(TxEvent.AFTER_COMMIT, self._handle_after_tx_commit, self._loop)
            tx._add_callback(TxEvent.AFTER_ROLLBACK, self._handle_after_tx_rollback, self._loop)

    async def _commit_batches_with_tx(self, tx: "BaseQueryTxContext"):
        grouped_batches = defaultdict(lambda: defaultdict(list))
        for batch in self._tx_to_batches_map[tx.tx_id]:
            grouped_batches[batch._partition_session.topic_path][batch._partition_session.partition_id].append(batch)

        request = UpdateOffsetsInTransactionRequest(tx=tx._tx_identity(), consumer=self._settings.consumer, topics=[])

        for topic_path in grouped_batches:
            topic_offsets = UpdateOffsetsInTransactionRequest.TopicOffsets(path=topic_path, partitions=[])
            for partition_id in grouped_batches[topic_path]:
                partition_offsets = UpdateOffsetsInTransactionRequest.TopicOffsets.PartitionOffsets(
                    partition_id=partition_id,
                    partition_offsets=[
                        batch._commit_get_offsets_range() for batch in grouped_batches[topic_path][partition_id]
                    ],
                )
                topic_offsets.partitions.append(partition_offsets)
            request.topics.append(topic_offsets)

        try:
            return await self._do_commit_batches_with_tx_call(request)
        except BaseException:
            exc = issues.ClientInternalError("Failed to update offsets in tx.")
            tx._set_external_error(exc)
            self._stream_reader._set_first_error(exc)
        finally:
            del self._tx_to_batches_map[tx.tx_id]

    async def _do_commit_batches_with_tx_call(self, request: UpdateOffsetsInTransactionRequest):
        args = [
            request.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.UpdateOffsetsInTransaction,
            topic_common.wrap_operation,
        ]

        if asyncio.iscoroutinefunction(self._driver.__call__):
            res = await self._driver(*args)
        else:
            res = await to_thread(self._driver, *args, executor=None)

        return res

    async def _handle_after_tx_rollback(self, tx: "BaseQueryTxContext", exc: Optional[BaseException]) -> None:
        if tx.tx_id in self._tx_to_batches_map:
            del self._tx_to_batches_map[tx.tx_id]
        exc = issues.ClientInternalError("Reconnect due to transaction rollback")
        self._stream_reader._set_first_error(exc)

    async def _handle_after_tx_commit(self, tx: "BaseQueryTxContext", exc: Optional[BaseException]) -> None:
        if tx.tx_id in self._tx_to_batches_map:
            del self._tx_to_batches_map[tx.tx_id]

        if exc is not None:
            self._stream_reader._set_first_error(
                issues.ClientInternalError("Reconnect due to transaction commit failed")
            )

    def commit(self, batch: datatypes.ICommittable) -> datatypes.PartitionSession.CommitAckWaiter:
        return self._stream_reader.commit(batch)

    async def close(self, flush: bool):
        logger.debug("reader reconnector %s close", self._id)
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

    @property
    def read_session_id(self) -> Optional[str]:
        if not self._stream_reader:
            return None
        return self._stream_reader._session_id


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
    _message_batches: typing.Dict[int, datatypes.PublicBatch]  # keys are partition session ID
    _first_error: asyncio.Future[YdbError]

    _update_token_interval: Union[int, float]
    _update_token_event: asyncio.Event
    _get_token_function: Callable[[], str]
    _settings: topic_reader.PublicReaderSettings

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
        self._message_batches = OrderedDict()

        self._update_token_interval = settings.update_token_interval
        self._get_token_function = get_token_function
        self._update_token_event = asyncio.Event()

        self._settings = settings

        logger.debug("created ReaderStream id=%s reconnector=%s", self._id, self._reader_reconnector_id)

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
        logger.debug("reader stream %s started session=%s", reader._id, reader._session_id)
        return reader

    async def _start(self, stream: IGrpcWrapperAsyncIO, init_message: StreamReadMessage.InitRequest):
        if self._started:
            raise TopicReaderError("Double start ReaderStream")

        self._started = True
        self._stream = stream
        logger.debug("reader stream %s send init request", self._id)

        stream.write(StreamReadMessage.FromClient(client_message=init_message))
        try:
            init_response = await stream.receive(
                timeout=DEFAULT_INITIAL_RESPONSE_TIMEOUT
            )  # type: StreamReadMessage.FromServer
        except asyncio.TimeoutError:
            raise TopicReaderError("Timeout waiting for init response")

        if isinstance(init_response.server_message, StreamReadMessage.InitResponse):
            self._session_id = init_response.server_message.session_id
            logger.debug("reader stream %s initialized session=%s", self._id, self._session_id)
        else:
            raise TopicReaderError("Unexpected message after InitRequest: %s", init_response)

        self._update_token_event.set()

        self._background_tasks.add(
            topic_common.wrap_set_name_for_asyncio_task(
                asyncio.create_task(self._read_messages_loop()),
                task_name="read_messages_loop",
            ),
        )
        self._background_tasks.add(
            topic_common.wrap_set_name_for_asyncio_task(
                asyncio.create_task(self._decode_batches_loop()),
                task_name="decode_batches",
            ),
        )
        if self._get_token_function:
            self._background_tasks.add(
                topic_common.wrap_set_name_for_asyncio_task(
                    asyncio.create_task(self._update_token_loop()),
                    task_name="update_token_loop",
                ),
            )
        self._background_tasks.add(
            topic_common.wrap_set_name_for_asyncio_task(
                asyncio.create_task(self._handle_background_errors()),
                task_name="handle_background_errors",
            ),
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

    def _get_first_batch(self) -> typing.Tuple[int, datatypes.PublicBatch]:
        partition_session_id, batch = self._message_batches.popitem(last=False)
        return partition_session_id, batch

    def _return_batch_to_queue(self, part_sess_id: int, batch: datatypes.PublicBatch):
        self._message_batches[part_sess_id] = batch

        # In case of auto-split we should return all parent messages ASAP
        # without queue rotation to prevent child's messages before parent's.
        if part_sess_id in self._partition_sessions and self._partition_sessions[part_sess_id].ended:
            self._message_batches.move_to_end(part_sess_id, last=False)

    def receive_batch_nowait(self, max_messages: Optional[int] = None):
        if self._get_first_error():
            raise self._get_first_error()

        if not self._message_batches:
            return None

        part_sess_id, batch = self._get_first_batch()

        if max_messages is None or len(batch.messages) <= max_messages:
            self._buffer_release_bytes(batch._bytes_size)
            return batch

        cutted_batch = batch._pop_batch(message_count=max_messages)

        self._return_batch_to_queue(part_sess_id, batch)

        self._buffer_release_bytes(cutted_batch._bytes_size)

        return cutted_batch

    def receive_message_nowait(self):
        if self._get_first_error():
            raise self._get_first_error()

        if not self._message_batches:
            return None

        part_sess_id, batch = self._get_first_batch()

        message, msgs_left = batch._pop()

        if not msgs_left:
            self._buffer_release_bytes(batch._bytes_size)
        else:
            # TODO: we should somehow release bytes from single message as well
            self._return_batch_to_queue(part_sess_id, batch)

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
            logger.debug("reader stream %s start read loop", self._id)
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
                        logger.debug("reader stream %s read %s bytes", self._id, message.server_message.bytes_size)
                        self._on_read_response(message.server_message)

                    elif isinstance(message.server_message, StreamReadMessage.CommitOffsetResponse):
                        self._on_commit_response(message.server_message)

                    elif isinstance(
                        message.server_message,
                        StreamReadMessage.StartPartitionSessionRequest,
                    ):
                        logger.debug(
                            "reader stream %s start partition %s",
                            self._id,
                            message.server_message.partition_session.partition_session_id,
                        )
                        await self._on_start_partition_session(message.server_message)

                    elif isinstance(
                        message.server_message,
                        StreamReadMessage.StopPartitionSessionRequest,
                    ):
                        logger.debug(
                            "reader stream %s stop partition %s",
                            self._id,
                            message.server_message.partition_session_id,
                        )
                        self._on_partition_session_stop(message.server_message)

                    elif isinstance(
                        message.server_message,
                        StreamReadMessage.EndPartitionSession,
                    ):
                        logger.debug(
                            "reader stream %s end partition %s",
                            self._id,
                            message.server_message.partition_session_id,
                        )
                        self._on_end_partition_session(message.server_message)

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
            logger.debug("reader stream %s error: %s", self._id, e)
            self._set_first_error(e)
            return

    async def _update_token_loop(self):
        while True:
            await asyncio.sleep(self._update_token_interval)
            token = self._get_token_function()
            if asyncio.iscoroutine(token):
                token = await token
            await self._update_token(token=token)

    async def _update_token(self, token: str):
        await self._update_token_event.wait()
        try:
            msg = StreamReadMessage.FromClient(UpdateTokenRequest(token))
            self._stream.write(msg)
        finally:
            self._update_token_event.clear()

    async def _on_start_partition_session(self, message: StreamReadMessage.StartPartitionSessionRequest):
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

            read_offset = None

            if self._settings.event_handler is not None:
                resp = await self._settings.event_handler._dispatch(
                    events.OnPartitionGetStartOffsetRequest(
                        message.partition_session.path,
                        message.partition_session.partition_id,
                    )
                )
                read_offset = None if resp is None else resp.start_offset

            self._stream.write(
                StreamReadMessage.FromClient(
                    client_message=StreamReadMessage.StartPartitionSessionResponse(
                        partition_session_id=message.partition_session.partition_session_id,
                        read_offset=read_offset,
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

    def _on_end_partition_session(self, message: StreamReadMessage.EndPartitionSession):
        logger.debug(
            f"End partition session with id: {message.partition_session_id}, "
            f"child partitions: {message.child_partition_ids}"
        )

        if message.partition_session_id in self._partition_sessions:
            # Mark partition session as ended not to shuffle messages.
            self._partition_sessions[message.partition_session_id].end()

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
                        metadata_items=message_data.metadata_items,
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
            logger.debug("reader stream %s decode batch %s messages", self._id, len(batch.messages))
            await self._decode_batch_inplace(batch)
            self._add_batch_to_queue(batch)
            self._state_changed.set()

    def _add_batch_to_queue(self, batch: datatypes.PublicBatch):
        part_sess_id = batch._partition_session.id
        if part_sess_id in self._message_batches:
            self._message_batches[part_sess_id]._extend(batch)
            logger.debug(
                "reader stream %s extend batch partition=%s size=%s",
                self._id,
                part_sess_id,
                len(batch.messages),
            )
            return

        self._message_batches[part_sess_id] = batch
        logger.debug(
            "reader stream %s new batch partition=%s size=%s",
            self._id,
            part_sess_id,
            len(batch.messages),
        )

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
        logger.debug("reader stream %s close", self._id)

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

        logger.debug("reader stream %s was closed", self._id)
