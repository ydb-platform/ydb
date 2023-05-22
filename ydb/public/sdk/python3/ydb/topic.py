from __future__ import annotations

__all__ = [
    "TopicClient",
    "TopicClientAsyncIO",
    "TopicClientSettings",
    "TopicCodec",
    "TopicConsumer",
    "TopicDescription",
    "TopicError",
    "TopicMeteringMode",
    "TopicReader",
    "TopicReaderAsyncIO",
    "TopicReaderSelector",
    "TopicReaderSettings",
    "TopicStatWindow",
    "TopicWriteResult",
    "TopicWriter",
    "TopicWriterAsyncIO",
    "TopicWriterInitInfo",
    "TopicWriterMessage",
    "TopicWriterSettings",
]

import concurrent.futures
import datetime
from dataclasses import dataclass
from typing import List, Union, Mapping, Optional, Dict, Callable

from . import aio, Credentials, _apis, issues

from . import driver

from ._topic_reader.topic_reader import (
    PublicReaderSettings as TopicReaderSettings,
    PublicTopicSelector as TopicReaderSelector,
)

from ._topic_reader.topic_reader_sync import TopicReaderSync as TopicReader

from ._topic_reader.topic_reader_asyncio import (
    PublicAsyncIOReader as TopicReaderAsyncIO,
)

from ._topic_writer.topic_writer import (  # noqa: F401
    PublicWriterSettings as TopicWriterSettings,
    PublicMessage as TopicWriterMessage,
    RetryPolicy as TopicWriterRetryPolicy,
    PublicWriterInitInfo as TopicWriterInitInfo,
    PublicWriteResult as TopicWriteResult,
)

from ydb._topic_writer.topic_writer_asyncio import WriterAsyncIO as TopicWriterAsyncIO
from ._topic_writer.topic_writer_sync import WriterSync as TopicWriter

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
    PublicMeteringMode as TopicMeteringMode,
)


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
        self.close()

    async def create_topic(
        self,
        path: str,
        min_active_partitions: Optional[int] = None,
        partition_count_limit: Optional[int] = None,
        retention_period: Optional[datetime.timedelta] = None,
        retention_storage_mb: Optional[int] = None,
        supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        partition_write_speed_bytes_per_second: Optional[int] = None,
        partition_write_burst_bytes: Optional[int] = None,
        attributes: Optional[Dict[str, str]] = None,
        consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        metering_mode: Optional[TopicMeteringMode] = None,
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

    async def describe_topic(self, path: str, include_stats: bool = False) -> TopicDescription:
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
        consumer: str,
        buffer_size_bytes: int = 50 * 1024 * 1024,
        # decoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None,
        decoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicReaderAsyncIO:

        if not decoder_executor:
            decoder_executor = self._executor

        args = locals()
        del args["self"]

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
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        encoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicWriterAsyncIO:
        args = locals()
        del args["self"]

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicWriterAsyncIO(self._driver, settings, _client=self)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._executor.shutdown(wait=False)

    def _check_closed(self):
        if not self._closed:
            return

        raise RuntimeError("Topic client closed")


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
        self.close()

    def create_topic(
        self,
        path: str,
        min_active_partitions: Optional[int] = None,
        partition_count_limit: Optional[int] = None,
        retention_period: Optional[datetime.timedelta] = None,
        retention_storage_mb: Optional[int] = None,
        supported_codecs: Optional[List[Union[TopicCodec, int]]] = None,
        partition_write_speed_bytes_per_second: Optional[int] = None,
        partition_write_burst_bytes: Optional[int] = None,
        attributes: Optional[Dict[str, str]] = None,
        consumers: Optional[List[Union[TopicConsumer, str]]] = None,
        metering_mode: Optional[TopicMeteringMode] = None,
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

    def describe_topic(self, path: str, include_stats: bool = False) -> TopicDescription:
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
        consumer: str,
        buffer_size_bytes: int = 50 * 1024 * 1024,
        # decoders: map[codec_code] func(encoded_bytes)->decoded_bytes
        decoders: Union[Mapping[int, Callable[[bytes], bytes]], None] = None,
        decoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicReader:
        if not decoder_executor:
            decoder_executor = self._executor

        args = locals()
        del args["self"]
        self._check_closed()

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
        encoders: Optional[Mapping[_ydb_topic_public_types.PublicCodec, Callable[[bytes], bytes]]] = None,
        encoder_executor: Optional[concurrent.futures.Executor] = None,  # default shared client executor pool
    ) -> TopicWriter:
        args = locals()
        del args["self"]
        self._check_closed()

        settings = TopicWriterSettings(**args)

        if not settings.encoder_executor:
            settings.encoder_executor = self._executor

        return TopicWriter(self._driver, settings, _parent=self)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._executor.shutdown(wait=False)

    def _check_closed(self):
        if not self._closed:
            return

        raise RuntimeError("Topic client closed")


@dataclass
class TopicClientSettings:
    encode_decode_threads_count: int = 4


class TopicError(issues.Error):
    pass
