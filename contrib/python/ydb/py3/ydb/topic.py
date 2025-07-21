from __future__ import annotations

__all__ = [
    "TopicClient",
    "TopicClientAsyncIO",
    "TopicClientSettings",
    "TopicCodec",
    "TopicConsumer",
    "TopicAlterConsumer",
    "TopicAlterAutoPartitioningSettings",
    "TopicAutoPartitioningSettings",
    "TopicAutoPartitioningStrategy",
    "TopicDescription",
    "TopicError",
    "TopicMeteringMode",
    "TopicReader",
    "TopicReaderAsyncIO",
    "TopicReaderBatch",
    "TopicReaderEvents",
    "TopicReaderMessage",
    "TopicReaderSelector",
    "TopicReaderSettings",
    "TopicReaderUnexpectedCodecError",
    "TopicReaderPartitionExpiredError",
    "TopicStatWindow",
    "TopicWriteResult",
    "TopicWriter",
    "TopicWriterAsyncIO",
    "TopicTxWriter",
    "TopicTxWriterAsyncIO",
    "TopicWriterInitInfo",
    "TopicWriterMessage",
    "TopicWriterSettings",
]

import concurrent.futures
import datetime
from dataclasses import dataclass
import logging
from typing import List, Union, Mapping, Optional, Dict, Callable

from . import aio, Credentials, _apis, issues

from . import driver

from ._topic_reader import events as TopicReaderEvents

from ._topic_reader.datatypes import (
    PublicBatch as TopicReaderBatch,
    PublicMessage as TopicReaderMessage,
)

from ._topic_reader.topic_reader import (
    PublicReaderSettings as TopicReaderSettings,
    PublicTopicSelector as TopicReaderSelector,
)

from ._topic_reader.topic_reader_sync import (
    TopicReaderSync as TopicReader,
)

from ._topic_reader.topic_reader_asyncio import (
    PublicAsyncIOReader as TopicReaderAsyncIO,
    PublicTopicReaderPartitionExpiredError as TopicReaderPartitionExpiredError,
    PublicTopicReaderUnexpectedCodecError as TopicReaderUnexpectedCodecError,
)

from ._topic_writer.topic_writer import (  # noqa: F401
    PublicWriterSettings as TopicWriterSettings,
    PublicMessage as TopicWriterMessage,
    RetryPolicy as TopicWriterRetryPolicy,
    PublicWriterInitInfo as TopicWriterInitInfo,
    PublicWriteResult as TopicWriteResult,
)

from ydb._topic_writer.topic_writer_asyncio import TxWriterAsyncIO as TopicTxWriterAsyncIO
from ydb._topic_writer.topic_writer_asyncio import WriterAsyncIO as TopicWriterAsyncIO
from ._topic_writer.topic_writer_sync import WriterSync as TopicWriter
from ._topic_writer.topic_writer_sync import TxWriterSync as TopicTxWriter

from ._topic_common.common import (
    wrap_operation as _wrap_operation,
    create_result_wrapper as _create_result_wrapper,
)

from ._grpc.grpcwrapper import ydb_topic as _ydb_topic
from ._grpc.grpcwrapper import ydb_topic_public_types as _ydb_topic_public_types
from ._grpc.grpcwrapper.ydb_topic_public_types import (  # noqa: F401
    PublicDescribeTopicResult as TopicDescription,
    PublicMultipleWindowsStat as TopicStatWindow,
    PublicPartitionStats as TopicPartitionStats,
    PublicCodec as TopicCodec,
    PublicConsumer as TopicConsumer,
    PublicAlterConsumer as TopicAlterConsumer,
    PublicMeteringMode as TopicMeteringMode,
    PublicAutoPartitioningStrategy as TopicAutoPartitioningStrategy,
    PublicAutoPartitioningSettings as TopicAutoPartitioningSettings,
    PublicAlterAutoPartitioningSettings as TopicAlterAutoPartitioningSettings,
)

logger = logging.getLogger(__name__)


class TopicClientAsyncIO:
    _closed: bool
    _driver: aio.Driver
    _credentials: Union[Credentials, None]
    _settings: TopicClientSettings
    _executor: concurrent.futures.Executor

    def __init__(self, driver: aio.Driver, settings: Optional[TopicClientSettings] = None):
        if not settings:
            settings = TopicClientSettings()
        self._closed = False
        self._driver = driver
        self._settings = settings
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.encode_decode_threads_count,
            thread_name_prefix="topic_asyncio_executor",
        )

    def __del__(self):
        if not self._closed:
            try:
                logger.debug("Topic client was not closed properly. Consider using method close().")
                self.close()
            except BaseException:
                logger.warning("Something went wrong during topic client close in __del__")

    async def create_topic(
        self,
        path: str,
        min_active_partitions: Optional[int] = None,
        max_active_partitions: Optional[int] = None,
        partition_count_limit: Optional[int] = None,
        retention_period: Optional[datetime.timedelta] = None,
        retention_storage_mb: Optional[int] = None,
        supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        partition_write_speed_bytes_per_second: Optional[int] = None,
        partition_write_burst_bytes: Optional[int] = None,
        attributes: Optional[Dict[str, str]] = None,
        consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        metering_mode: Optional[TopicMeteringMode] = None,
        auto_partitioning_settings: Optional[TopicAutoPartitioningSettings] = None,
    ):
        """
        create topic command

        :param path: full path to topic
        :param min_active_partitions: Minimum partition count auto merge would stop working at.
        :param partition_count_limit: Limit for total partition count, including active (open for write)
            and read-only partitions.
        :param retention_period: How long data in partition should be stored
        :param retention_storage_mb: How much data in partition should be stored
        :param supported_codecs: List of allowed codecs for writers. Writes with codec not from this list are forbidden.
            Empty list mean disable codec compatibility checks for the topic.
        :param partition_write_speed_bytes_per_second: Partition write speed in bytes per second
        :param partition_write_burst_bytes: Burst size for write in partition, in bytes
        :param attributes: User and server attributes of topic.
            Server attributes starts from "_" and will be validated by server.
        :param consumers: List of consumers for this topic
        :param metering_mode: Metering mode for the topic in a serverless database
        """
        logger.debug("Create topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        req = _ydb_topic_public_types.CreateTopicRequestParams(**args)
        req = _ydb_topic.CreateTopicRequest.from_public(req)
        await self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.CreateTopic,
            _wrap_operation,
        )

    async def alter_topic(
        self,
        path: str,
        set_min_active_partitions: Optional[int] = None,
        set_max_active_partitions: Optional[int] = None,
        set_partition_count_limit: Optional[int] = None,
        add_consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        alter_consumers: Optional[List[Union[TopicAlterConsumer, str]]] = None,
        drop_consumers: Optional[List[str]] = None,
        alter_attributes: Optional[Dict[str, str]] = None,
        set_metering_mode: Optional[TopicMeteringMode] = None,
        set_partition_write_speed_bytes_per_second: Optional[int] = None,
        set_partition_write_burst_bytes: Optional[int] = None,
        set_retention_period: Optional[datetime.timedelta] = None,
        set_retention_storage_mb: Optional[int] = None,
        set_supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        alter_auto_partitioning_settings: Optional[TopicAlterAutoPartitioningSettings] = None,
    ):
        """
        alter topic command

        :param path: full path to topic
        :param set_min_active_partitions: Minimum partition count auto merge would stop working at.
        :param set_partition_count_limit: Limit for total partition count, including active (open for write)
            and read-only partitions.
        :param add_consumers: List of consumers for this topic to add
        :param alter_consumers: List of consumers for this topic to alter
        :param drop_consumers: List of consumer names for this topic to drop
        :param alter_attributes: User and server attributes of topic.
            Server attributes starts from "_" and will be validated by server.
        :param set_metering_mode: Metering mode for the topic in a serverless database
        :param set_partition_write_speed_bytes_per_second: Partition write speed in bytes per second
        :param set_partition_write_burst_bytes: Burst size for write in partition, in bytes
        :param set_retention_period: How long data in partition should be stored
        :param set_retention_storage_mb: How much data in partition should be stored
        :param set_supported_codecs: List of allowed codecs for writers. Writes with codec not from this list are forbidden.
            Empty list mean disable codec compatibility checks for the topic.
        """
        logger.debug("Alter topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        req = _ydb_topic_public_types.AlterTopicRequestParams(**args)
        req = _ydb_topic.AlterTopicRequest.from_public(req)
        await self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.AlterTopic,
            _wrap_operation,
        )

    async def describe_topic(self, path: str, include_stats: bool = False) -> TopicDescription:
        logger.debug("Describe topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        req = _ydb_topic_public_types.DescribeTopicRequestParams(**args)
        res = await self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.DescribeTopic,
            _create_result_wrapper(_ydb_topic.DescribeTopicResult),
        )  # type: _ydb_topic.DescribeTopicResult
        return res.to_public()

    async def drop_topic(self, path: str):
        logger.debug("Drop topic request: path=%s", path)
        req = _ydb_topic_public_types.DropTopicRequestParams(path=path)
        await self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.DropTopic,
            _wrap_operation,
        )

    def reader(
        self,
        topic: Union[str, TopicReaderSelector, List[Union[str, TopicReaderSelector]]],
        consumer: Optional[str],
        buffer_size_bytes: int = 50 * 1024 * 1024,
        # decoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel
        decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None,
        # custom decoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # if max_worker in the executor is 1 - then decoders will be called from the thread without parallel
        decoder_executor: Optional[concurrent.futures.Executor] = None,
        auto_partitioning_support: Optional[bool] = True,  # Auto partitioning feature flag. Default - True.
        event_handler: Optional[TopicReaderEvents.EventHandler] = None,
    ) -> TopicReaderAsyncIO:

        logger.debug("Create reader for topic=%s consumer=%s", topic, consumer)

        if not decoder_executor:
            decoder_executor = self._executor

        args = locals().copy()
        del args["self"]

        if consumer == "":
            raise issues.Error(
                "Consumer name could not be empty! To use reader without consumer specify consumer as None."
            )

        if consumer is None:
            if not isinstance(topic, TopicReaderSelector) or topic.partitions is None:
                raise issues.Error(
                    "To use reader without consumer it is required to specify partition_ids in topic selector."
                )

            if event_handler is None:
                raise issues.Error(
                    "To use reader without consumer it is required to specify event_handler with "
                    "on_partition_get_start_offset method."
                )

        settings = TopicReaderSettings(**args)

        return TopicReaderAsyncIO(self._driver, settings, _parent=self)

    def writer(
        self,
        topic,
        *,
        producer_id: Optional[str] = None,  # default - random
        session_metadata: Mapping[str, str] = None,
        partition_id: Union[int, None] = None,
        auto_seqno: bool = True,
        auto_created_at: bool = True,
        codec: Optional[TopicCodec] = None,  # default mean auto-select
        # encoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel.
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        # custom encoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # If max_worker in the executor is 1 - then encoders will be called from the thread without parallel.
        encoder_executor: Optional[concurrent.futures.Executor] = None,
    ) -> TopicWriterAsyncIO:
        logger.debug("Create writer for topic=%s producer_id=%s", topic, producer_id)
        args = locals().copy()
        del args["self"]

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicWriterAsyncIO(self._driver, settings, _client=self)

    def tx_writer(
        self,
        tx,
        topic,
        *,
        producer_id: Optional[str] = None,  # default - random
        session_metadata: Mapping[str, str] = None,
        partition_id: Union[int, None] = None,
        auto_seqno: bool = True,
        auto_created_at: bool = True,
        codec: Optional[TopicCodec] = None,  # default mean auto-select
        # encoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel.
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        # custom encoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # If max_worker in the executor is 1 - then encoders will be called from the thread without parallel.
        encoder_executor: Optional[concurrent.futures.Executor] = None,
    ) -> TopicTxWriterAsyncIO:
        logger.debug("Create tx writer for topic=%s tx=%s", topic, tx)
        args = locals().copy()
        del args["self"]
        del args["tx"]

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicTxWriterAsyncIO(tx=tx, driver=self._driver, settings=settings, _client=self)

    async def commit_offset(
        self, path: str, consumer: str, partition_id: int, offset: int, read_session_id: Optional[str] = None
    ) -> None:
        logger.debug(
            "Commit offset: path=%s partition_id=%s offset=%s consumer=%s",
            path,
            partition_id,
            offset,
            consumer,
        )
        req = _ydb_topic.CommitOffsetRequest(
            path=path,
            consumer=consumer,
            partition_id=partition_id,
            offset=offset,
            read_session_id=read_session_id,
        )

        await self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.CommitOffset,
            _wrap_operation,
        )

    def close(self):
        if self._closed:
            return

        logger.debug("Close topic client")
        self._closed = True
        self._executor.shutdown(wait=False)

    def _check_closed(self):
        if not self._closed:
            return

        raise issues.Error("Topic client closed")


class TopicClient:
    _closed: bool
    _driver: driver.Driver
    _credentials: Union[Credentials, None]
    _settings: TopicClientSettings
    _executor: concurrent.futures.Executor

    def __init__(self, driver: driver.Driver, settings: Optional[TopicClientSettings]):
        if not settings:
            settings = TopicClientSettings()

        self._closed = False
        self._driver = driver
        self._settings = settings
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.encode_decode_threads_count,
            thread_name_prefix="topic_asyncio_executor",
        )

    def __del__(self):
        if not self._closed:
            try:
                logger.warning("Topic client was not closed properly. Consider using method close().")
                self.close()
            except BaseException:
                logger.warning("Something went wrong during topic client close in __del__")

    def create_topic(
        self,
        path: str,
        min_active_partitions: Optional[int] = None,
        max_active_partitions: Optional[int] = None,
        partition_count_limit: Optional[int] = None,
        retention_period: Optional[datetime.timedelta] = None,
        retention_storage_mb: Optional[int] = None,
        supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        partition_write_speed_bytes_per_second: Optional[int] = None,
        partition_write_burst_bytes: Optional[int] = None,
        attributes: Optional[Dict[str, str]] = None,
        consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        metering_mode: Optional[TopicMeteringMode] = None,
        auto_partitioning_settings: Optional[TopicAutoPartitioningSettings] = None,
    ):
        """
        create topic command

        :param path: full path to topic
        :param min_active_partitions: Minimum partition count auto merge would stop working at.
        :param partition_count_limit: Limit for total partition count, including active (open for write)
            and read-only partitions.
        :param retention_period: How long data in partition should be stored
        :param retention_storage_mb: How much data in partition should be stored
        :param supported_codecs: List of allowed codecs for writers. Writes with codec not from this list are forbidden.
            Empty list mean disable codec compatibility checks for the topic.
        :param partition_write_speed_bytes_per_second: Partition write speed in bytes per second
        :param partition_write_burst_bytes: Burst size for write in partition, in bytes
        :param attributes: User and server attributes of topic.
            Server attributes starts from "_" and will be validated by server.
        :param consumers: List of consumers for this topic
        :param metering_mode: Metering mode for the topic in a serverless database
        """
        logger.debug("Create topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        self._check_closed()

        req = _ydb_topic_public_types.CreateTopicRequestParams(**args)
        req = _ydb_topic.CreateTopicRequest.from_public(req)
        self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.CreateTopic,
            _wrap_operation,
        )

    def alter_topic(
        self,
        path: str,
        set_min_active_partitions: Optional[int] = None,
        set_max_active_partitions: Optional[int] = None,
        set_partition_count_limit: Optional[int] = None,
        add_consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        alter_consumers: Optional[List[Union[TopicAlterConsumer, str]]] = None,
        drop_consumers: Optional[List[str]] = None,
        alter_attributes: Optional[Dict[str, str]] = None,
        set_metering_mode: Optional[TopicMeteringMode] = None,
        set_partition_write_speed_bytes_per_second: Optional[int] = None,
        set_partition_write_burst_bytes: Optional[int] = None,
        set_retention_period: Optional[datetime.timedelta] = None,
        set_retention_storage_mb: Optional[int] = None,
        set_supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        alter_auto_partitioning_settings: Optional[TopicAlterAutoPartitioningSettings] = None,
    ):
        """
        alter topic command

        :param path: full path to topic
        :param set_min_active_partitions: Minimum partition count auto merge would stop working at.
        :param set_partition_count_limit: Limit for total partition count, including active (open for write)
            and read-only partitions.
        :param add_consumers: List of consumers for this topic to add
        :param alter_consumers: List of consumers for this topic to alter
        :param drop_consumers: List of consumer names for this topic to drop
        :param alter_attributes: User and server attributes of topic.
            Server attributes starts from "_" and will be validated by server.
        :param set_metering_mode: Metering mode for the topic in a serverless database
        :param set_partition_write_speed_bytes_per_second: Partition write speed in bytes per second
        :param set_partition_write_burst_bytes: Burst size for write in partition, in bytes
        :param set_retention_period: How long data in partition should be stored
        :param set_retention_storage_mb: How much data in partition should be stored
        :param set_supported_codecs: List of allowed codecs for writers. Writes with codec not from this list are forbidden.
            Empty list mean disable codec compatibility checks for the topic.
        """
        logger.debug("Alter topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        self._check_closed()

        req = _ydb_topic_public_types.AlterTopicRequestParams(**args)
        req = _ydb_topic.AlterTopicRequest.from_public(req)
        self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.AlterTopic,
            _wrap_operation,
        )

    def describe_topic(self, path: str, include_stats: bool = False) -> TopicDescription:
        logger.debug("Describe topic request: path=%s", path)
        args = locals().copy()
        del args["self"]
        self._check_closed()

        req = _ydb_topic_public_types.DescribeTopicRequestParams(**args)
        res = self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.DescribeTopic,
            _create_result_wrapper(_ydb_topic.DescribeTopicResult),
        )  # type: _ydb_topic.DescribeTopicResult
        return res.to_public()

    def drop_topic(self, path: str):
        self._check_closed()

        logger.debug("Drop topic request: path=%s", path)

        req = _ydb_topic_public_types.DropTopicRequestParams(path=path)
        self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.DropTopic,
            _wrap_operation,
        )

    def reader(
        self,
        topic: Union[str, TopicReaderSelector, List[Union[str, TopicReaderSelector]]],
        consumer: Optional[str],
        buffer_size_bytes: int = 50 * 1024 * 1024,
        # decoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel
        decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None,
        # custom decoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # if max_worker in the executor is 1 - then decoders will be called from the thread without parallel
        decoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
        auto_partitioning_support: Optional[bool] = True,  # Auto partitioning feature flag. Default - True.
        event_handler: Optional[TopicReaderEvents.EventHandler] = None,
    ) -> TopicReader:
        logger.debug("Create reader for topic=%s consumer=%s", topic, consumer)
        if not decoder_executor:
            decoder_executor = self._executor

        args = locals().copy()
        del args["self"]

        if consumer == "":
            raise issues.Error(
                "Consumer name could not be empty! To use reader without consumer specify consumer as None."
            )

        if consumer is None:
            if not isinstance(topic, TopicReaderSelector) or topic.partitions is None:
                raise issues.Error(
                    "To use reader without consumer it is required to specify partition_ids in topic selector."
                )

            if event_handler is None:
                raise issues.Error(
                    "To use reader without consumer it is required to specify event_handler with "
                    "on_partition_get_start_offset method."
                )

        settings = TopicReaderSettings(**args)

        return TopicReader(self._driver, settings, _parent=self)

    def writer(
        self,
        topic,
        *,
        producer_id: Optional[str] = None,  # default - random
        session_metadata: Mapping[str, str] = None,
        partition_id: Union[int, None] = None,
        auto_seqno: bool = True,
        auto_created_at: bool = True,
        codec: Optional[TopicCodec] = None,  # default mean auto-select
        # encoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel.
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        # custom encoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # If max_worker in the executor is 1 - then encoders will be called from the thread without parallel.
        encoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicWriter:
        logger.debug("Create writer for topic=%s producer_id=%s", topic, producer_id)
        args = locals().copy()
        del args["self"]
        self._check_closed()

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicWriter(self._driver, settings, _parent=self)

    def tx_writer(
        self,
        tx,
        topic,
        *,
        producer_id: Optional[str] = None,  # default - random
        session_metadata: Mapping[str, str] = None,
        partition_id: Union[int, None] = None,
        auto_seqno: bool = True,
        auto_created_at: bool = True,
        codec: Optional[TopicCodec] = None,  # default mean auto-select
        # encoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        # the func will be called from multiply threads in parallel.
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        # custom encoder executor for call builtin and custom decoders. If None - use shared executor pool.
        # If max_worker in the executor is 1 - then encoders will be called from the thread without parallel.
        encoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicWriter:
        logger.debug("Create tx writer for topic=%s tx=%s", topic, tx)
        args = locals().copy()
        del args["self"]
        del args["tx"]
        self._check_closed()

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicTxWriter(tx, self._driver, settings, _parent=self)

    def commit_offset(
        self, path: str, consumer: str, partition_id: int, offset: int, read_session_id: Optional[str] = None
    ) -> None:
        logger.debug(
            "Commit offset: path=%s partition_id=%s offset=%s consumer=%s",
            path,
            partition_id,
            offset,
            consumer,
        )
        req = _ydb_topic.CommitOffsetRequest(
            path=path,
            consumer=consumer,
            partition_id=partition_id,
            offset=offset,
            read_session_id=read_session_id,
        )

        self._driver(
            req.to_proto(),
            _apis.TopicService.Stub,
            _apis.TopicService.CommitOffset,
            _wrap_operation,
        )

    def close(self):
        if self._closed:
            return

        logger.debug("Close topic client")
        self._closed = True
        self._executor.shutdown(wait=False)
        logger.debug("Topic client was closed")

    def _check_closed(self):
        if not self._closed:
            return

        raise issues.Error("Topic client closed")


@dataclass
class TopicClientSettings:
    # ATTENTION
    # When set the encode_decode_threads_count - all custom encoders/decoders for topic reader/writer
    # MUST be thread-safe
    # because they will be called from parallel threads
    encode_decode_threads_count: int = 1


class TopicError(issues.Error):
    pass
