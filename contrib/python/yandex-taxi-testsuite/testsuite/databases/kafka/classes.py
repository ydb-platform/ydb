import asyncio
import dataclasses
import logging
import typing

import aiokafka

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ServiceSettings:
    """Kafka service start settings"""

    server_host: str
    server_port: int
    controller_port: int
    custom_start_topics: dict[str, int]


"""Kafka bootstrap servers URLs list"""
BootstrapServers = list[str]

"""Kafka message header"""
Header = tuple[str, bytes]

"""
    Kafka headers sequence.
    The order is taken into account.
    Duplicate keys are allowed.
"""
Headers = typing.Sequence[Header]


class KafkaDisabledError(Exception):
    pass


class KafkaProducer:
    """
    Kafka producer wrapper.
    """

    def __init__(self, enabled: bool, bootstrap_servers: str):
        self._enabled = enabled
        self._bootstrap_servers = bootstrap_servers

    async def start(self):
        if self._enabled:
            self.producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                linger_ms=0,  # turn off message buffering
            )
            await self.producer.start()

    async def send(
        self,
        topic: str,
        key: str | bytes,
        value: str | bytes,
        partition: int | None = None,
        headers: Headers | None = None,
    ):
        """
        Sends the message (``value``) to ``topic`` by ``key`` and,
        optionally, to a given ``partition`` and waits until it is delivered.
        If the call is successfully awaited,
        message is guaranteed to be delivered.

        :param topic: topic name.
        :param key: key. Needed to determine message's partition.
        :param value: message payload. Must be valid UTF-8.
        :param partition: Optional message partition.
            If not passed, determined by internal partitioner
            depends on key's hash.
        """

        resp_future = await self.send_async(
            topic, key, value, partition, headers
        )
        await resp_future

    async def send_async(
        self,
        topic: str,
        key: str | bytes,
        value: str | bytes,
        partition: int | None = None,
        headers: Headers | None = None,
    ):
        """
        Sends the message (``value``) to ``topic`` by ``key`` and,
        optionally, to a given ``partition`` and
        returns the future for message delivery awaiting.

        :param topic: topic name.
        :param key: key. Needed to determine message's partition.
        :param value: message payload. Must be valid UTF-8.
        :param partition: Optional message partition.
            If not passed, determined by internal partitioner
            depends on key's hash.
        """

        if not self._enabled:
            raise KafkaDisabledError

        return await self.producer.send(
            topic=topic,
            value=value if isinstance(value, bytes) else value.encode(),
            key=key if isinstance(key, bytes) else key.encode(),
            partition=partition,
            headers=headers,
        )

    async def _flush(self):
        logger.info('Flusing produced messages')
        await self.producer.flush()

    async def aclose(self):
        if self._enabled:
            await self.producer.stop()


class ConsumedMessage:
    """Wrapper for consumed record."""

    topic: str
    key_raw: bytes
    value_raw: bytes
    partition: int
    offset: int
    headers_raw: Headers

    def __init__(self, record: aiokafka.ConsumerRecord):
        self.topic = record.topic
        self.key_raw = record.key
        self.value_raw = record.value
        self.partition = record.partition
        self.offset = record.offset
        self.headers_raw = record.headers

    @property
    def key(self) -> str:
        return self.key_raw.decode()

    @property
    def value(self) -> str:
        return self.value_raw.decode()

    @property
    def headers(self) -> list[Header]:
        return list(self.headers_raw)


class KafkaConsumer:
    """
    Kafka balanced consumer wrapper.
    All consumers are created with the same group.id,
    after each test consumer commits offsets for all consumed messages.
    This is needed to make tests independent.
    """

    def __init__(self, enabled: bool, bootstrap_servers):
        self._enabled = enabled
        self._bootstrap_servers = bootstrap_servers
        self._subscribed_topics: list[str] = []

    async def start(self):
        if self._enabled:
            self.consumer = aiokafka.AIOKafkaConsumer(
                group_id='Test-group',
                bootstrap_servers=self._bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
            )
            await self.consumer.start()

    def _subscribe(self, topics: list[str]):
        if not self._enabled:
            raise KafkaDisabledError

        to_subscribe: list[str] = []
        for topic in topics:
            if topic not in self._subscribed_topics:
                to_subscribe.append(topic)

        if to_subscribe:
            logger.info('Subscribing to [%s]', ','.join(to_subscribe))
            self.consumer.subscribe(to_subscribe)
            self._subscribed_topics.extend(to_subscribe)

    async def _commit(self):
        if not self._enabled:
            raise KafkaDisabledError

        if self._subscribed_topics:
            await self.consumer.commit()

    async def _unsubscribe(self):
        await self._commit()

        if self._subscribed_topics:
            logger.info('Unsubscribing from all topics')
            self.consumer.unsubscribe()
            self._subscribed_topics = []

    async def receive_one(
        self, topics: list[str], timeout: float = 20.0
    ) -> ConsumedMessage:
        """
        Waits until one message are consumed.

        :param topics: list of topics to read messages from.
        :param timeout: timeout to stop waiting. Default is 20 seconds.

        :returns: :py:class:`ConsumedMessage`
        """
        if not self._enabled:
            raise KafkaDisabledError

        self._subscribe(topics)

        async def _do_receive():
            record: aiokafka.ConsumerRecord = await self.consumer.getone()
            return ConsumedMessage(record)

        return await asyncio.wait_for(_do_receive(), timeout=timeout)

    async def receive_batch(
        self,
        topics: list[str],
        max_batch_size: int | None,
        timeout: float = 3.0,
    ) -> list[ConsumedMessage]:
        """
        Waits until either ``max_batch_size`` messages are consumed or
        ``timeout`` expired.

        :param topics: list of topics to read messages from.
        :max_batch_size: maximum number of consumed messages.
        :param timeout: timeout to stop waiting. Default is 3 seconds.

        :returns: :py:class:`List[ConsumedMessage]`
        """
        if not self._enabled:
            raise KafkaDisabledError

        self._subscribe(topics)

        records: dict[
            aiokafka.TopicPartition, list[aiokafka.ConsumerRecord]
        ] = await self.consumer.getmany(
            timeout_ms=int(timeout * 1000), max_records=max_batch_size
        )

        return list(map(ConsumedMessage, sum(records.values(), [])))

    async def aclose(self):
        if self._enabled:
            await self._unsubscribe()
            await self.consumer.stop()
