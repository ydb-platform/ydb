from __future__ import annotations

import asyncio
import contextlib
import json
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    MutableSequence,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    cast,
)

import aiokafka

from opentelemetry import context, propagate, trace
from opentelemetry.context import Context
from opentelemetry.propagators import textmap
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from opentelemetry.semconv.attributes import server_attributes
from opentelemetry.trace import Tracer
from opentelemetry.trace.span import Span

if TYPE_CHECKING:
    from aiokafka.structs import RecordMetadata

    class AIOKafkaGetOneProto(Protocol):
        async def __call__(
            self, *partitions: aiokafka.TopicPartition
        ) -> aiokafka.ConsumerRecord[object, object]: ...

    class AIOKafkaGetManyProto(Protocol):
        async def __call__(
            self,
            *partitions: aiokafka.TopicPartition,
            timeout_ms: int = 0,
            max_records: int | None = None,
        ) -> dict[
            aiokafka.TopicPartition,
            list[aiokafka.ConsumerRecord[object, object]],
        ]: ...

    class AIOKafkaSendProto(Protocol):
        async def __call__(
            self,
            topic: str,
            value: object | None = None,
            key: object | None = None,
            partition: int | None = None,
            timestamp_ms: int | None = None,
            headers: HeadersT | None = None,
        ) -> asyncio.Future[RecordMetadata]: ...

    ProduceHookT = Callable[
        [Span, Tuple[Any, ...], Dict[str, Any]], Awaitable[None]
    ]

    ConsumeHookT = Callable[
        [
            Span,
            aiokafka.ConsumerRecord[object, object],
            Tuple[aiokafka.TopicPartition, ...],
            Dict[str, Any],
        ],
        Awaitable[None],
    ]

    HeadersT = Sequence[Tuple[str, Optional[bytes]]]

_LOG = getLogger(__name__)


def _extract_bootstrap_servers(
    client: aiokafka.AIOKafkaClient,
) -> str | list[str]:
    return client._bootstrap_servers


def _extract_client_id(client: aiokafka.AIOKafkaClient) -> str:
    return client._client_id


def _extract_consumer_group(
    consumer: aiokafka.AIOKafkaConsumer,
) -> str | None:
    return consumer._group_id  # type: ignore[reportUnknownVariableType]


def _extract_argument(
    key: str,
    position: int,
    default_value: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    if len(args) > position:
        return args[position]
    return kwargs.get(key, default_value)


def _extract_send_topic(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """extract topic from `send` method arguments in AIOKafkaProducer class"""
    return _extract_argument("topic", 0, "unknown", args, kwargs)


def _extract_send_value(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> object | None:
    """extract value from `send` method arguments in AIOKafkaProducer class"""
    return _extract_argument("value", 1, None, args, kwargs)


def _extract_send_key(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> object | None:
    """extract key from `send` method arguments in AIOKafkaProducer class"""
    return _extract_argument("key", 2, None, args, kwargs)


def _extract_send_headers(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> HeadersT | None:
    """extract headers from `send` method arguments in AIOKafkaProducer class"""
    return _extract_argument("headers", 5, None, args, kwargs)


def _move_headers_to_kwargs(
    args: Tuple[Any], kwargs: Dict[str, Any]
) -> Tuple[Tuple[Any], Dict[str, Any]]:
    """Move headers from args to kwargs"""
    if len(args) > 5:
        kwargs["headers"] = args[5]
    return args[:5], kwargs


def _deserialize_key(key: object | None) -> str | None:
    if key is None:
        return None

    if isinstance(key, bytes):
        with contextlib.suppress(UnicodeDecodeError):
            return key.decode()

    return str(key)


async def _extract_send_partition(
    instance: aiokafka.AIOKafkaProducer,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> int | None:
    """extract partition `send` method arguments, using the `_partition` method in AIOKafkaProducer class"""
    try:
        topic = _extract_send_topic(args, kwargs)
        key = _extract_send_key(args, kwargs)
        value = _extract_send_value(args, kwargs)
        partition = _extract_argument("partition", 3, None, args, kwargs)
        key_bytes, value_bytes = cast(
            "tuple[bytes | None, bytes | None]",
            instance._serialize(topic, key, value),  # type: ignore[reportUnknownMemberType]
        )
        valid_types = (bytes, bytearray, memoryview, type(None))
        if (
            type(key_bytes) not in valid_types
            or type(value_bytes) not in valid_types
        ):
            return None

        await instance.client._wait_on_metadata(topic)  # type: ignore[reportUnknownMemberType]

        return instance._partition(  # type: ignore[reportUnknownMemberType]
            topic, partition, key, value, key_bytes, value_bytes
        )
    except Exception as exception:  # pylint: disable=W0703
        _LOG.debug("Unable to extract partition: %s", exception)
        return None


class AIOKafkaContextGetter(textmap.Getter["HeadersT"]):
    def get(self, carrier: HeadersT, key: str) -> list[str] | None:
        for item_key, value in carrier:
            if item_key == key:
                if value is not None:
                    return [value.decode()]
        return None

    def keys(self, carrier: HeadersT) -> list[str]:
        return [key for (key, _) in carrier]


class AIOKafkaContextSetter(textmap.Setter["HeadersT"]):
    def set(
        self, carrier: HeadersT, key: str | None, value: str | None
    ) -> None:
        if key is None:
            return

        if not isinstance(carrier, MutableSequence):
            _LOG.warning(
                "Unable to set context in headers. Headers is immutable"
            )
            return

        if value is not None:
            carrier.append((key, value.encode()))
        else:
            carrier.append((key, value))


_aiokafka_getter = AIOKafkaContextGetter()
_aiokafka_setter = AIOKafkaContextSetter()


def _enrich_base_span(
    span: Span,
    *,
    bootstrap_servers: str | list[str],
    client_id: str,
    topic: str,
    partition: int | None,
    key: str | None,
) -> None:
    span.set_attribute(
        messaging_attributes.MESSAGING_SYSTEM,
        messaging_attributes.MessagingSystemValues.KAFKA.value,
    )
    span.set_attribute(
        server_attributes.SERVER_ADDRESS, json.dumps(bootstrap_servers)
    )
    span.set_attribute(messaging_attributes.MESSAGING_CLIENT_ID, client_id)
    span.set_attribute(messaging_attributes.MESSAGING_DESTINATION_NAME, topic)

    if partition is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_DESTINATION_PARTITION_ID,
            str(partition),
        )

    if key is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_KAFKA_MESSAGE_KEY, key
        )


def _enrich_send_span(
    span: Span,
    *,
    bootstrap_servers: str | list[str],
    client_id: str,
    topic: str,
    partition: int | None,
    key: str | None,
) -> None:
    if not span.is_recording():
        return

    _enrich_base_span(
        span,
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        topic=topic,
        partition=partition,
        key=key,
    )

    span.set_attribute(messaging_attributes.MESSAGING_OPERATION_NAME, "send")
    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_TYPE,
        messaging_attributes.MessagingOperationTypeValues.PUBLISH.value,
    )


def _enrich_getone_span(
    span: Span,
    *,
    bootstrap_servers: str | list[str],
    client_id: str,
    consumer_group: str | None,
    topic: str,
    partition: int | None,
    key: str | None,
    offset: int,
) -> None:
    if not span.is_recording():
        return

    _enrich_base_span(
        span,
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        topic=topic,
        partition=partition,
        key=key,
    )

    if consumer_group is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_CONSUMER_GROUP_NAME, consumer_group
        )

    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_NAME, "receive"
    )
    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_TYPE,
        messaging_attributes.MessagingOperationTypeValues.RECEIVE.value,
    )

    span.set_attribute(
        messaging_attributes.MESSAGING_KAFKA_MESSAGE_OFFSET, offset
    )

    # https://stackoverflow.com/questions/65935155/identify-and-find-specific-message-in-kafka-topic
    # A message within Kafka is uniquely defined by its topic name, topic partition and offset.
    if partition is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_MESSAGE_ID,
            f"{topic}.{partition}.{offset}",
        )


def _enrich_getmany_poll_span(
    span: Span,
    *,
    bootstrap_servers: str | list[str],
    client_id: str,
    consumer_group: str | None,
    message_count: int,
) -> None:
    if not span.is_recording():
        return

    span.set_attribute(
        messaging_attributes.MESSAGING_SYSTEM,
        messaging_attributes.MessagingSystemValues.KAFKA.value,
    )
    span.set_attribute(
        server_attributes.SERVER_ADDRESS, json.dumps(bootstrap_servers)
    )
    span.set_attribute(messaging_attributes.MESSAGING_CLIENT_ID, client_id)

    if consumer_group is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_CONSUMER_GROUP_NAME, consumer_group
        )

    span.set_attribute(
        messaging_attributes.MESSAGING_BATCH_MESSAGE_COUNT, message_count
    )

    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_NAME, "receive"
    )
    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_TYPE,
        messaging_attributes.MessagingOperationTypeValues.RECEIVE.value,
    )


def _enrich_getmany_topic_span(
    span: Span,
    *,
    bootstrap_servers: str | list[str],
    client_id: str,
    consumer_group: str | None,
    topic: str,
    partition: int,
    message_count: int,
) -> None:
    if not span.is_recording():
        return

    _enrich_base_span(
        span,
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        topic=topic,
        partition=partition,
        key=None,
    )

    if consumer_group is not None:
        span.set_attribute(
            messaging_attributes.MESSAGING_CONSUMER_GROUP_NAME, consumer_group
        )

    span.set_attribute(
        messaging_attributes.MESSAGING_BATCH_MESSAGE_COUNT, message_count
    )

    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_NAME, "receive"
    )
    span.set_attribute(
        messaging_attributes.MESSAGING_OPERATION_TYPE,
        messaging_attributes.MessagingOperationTypeValues.RECEIVE.value,
    )


def _get_span_name(operation: str, topic: str):
    return f"{topic} {operation}"


def _wrap_send(  # type: ignore[reportUnusedFunction]
    tracer: Tracer, async_produce_hook: ProduceHookT | None
) -> Callable[..., Awaitable[asyncio.Future[RecordMetadata]]]:
    async def _traced_send(
        func: AIOKafkaSendProto,
        instance: aiokafka.AIOKafkaProducer,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> asyncio.Future[RecordMetadata]:
        args, kwargs = _move_headers_to_kwargs(args, kwargs)
        headers: HeadersT | None = _extract_send_headers(args, kwargs)
        if headers is None:
            headers = []
            kwargs["headers"] = headers

        topic = _extract_send_topic(args, kwargs)
        bootstrap_servers = _extract_bootstrap_servers(instance.client)
        client_id = _extract_client_id(instance.client)
        key = _deserialize_key(_extract_send_key(args, kwargs))
        partition = await _extract_send_partition(instance, args, kwargs)
        span_name = _get_span_name("send", topic)
        with tracer.start_as_current_span(
            span_name, kind=trace.SpanKind.PRODUCER
        ) as span:
            _enrich_send_span(
                span,
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                topic=topic,
                partition=partition,
                key=key,
            )
            propagate.inject(
                headers,
                context=trace.set_span_in_context(span),
                setter=_aiokafka_setter,
            )
            try:
                if async_produce_hook is not None:
                    await async_produce_hook(span, args, kwargs)
            except Exception as hook_exception:  # pylint: disable=W0703
                _LOG.exception(hook_exception)

        return await func(*args, **kwargs)

    return _traced_send


async def _create_consumer_span(
    tracer: Tracer,
    async_consume_hook: ConsumeHookT | None,
    record: aiokafka.ConsumerRecord[object, object],
    extracted_context: Context,
    bootstrap_servers: str | list[str],
    client_id: str,
    consumer_group: str | None,
    args: tuple[aiokafka.TopicPartition, ...],
    kwargs: dict[str, Any],
) -> trace.Span:
    span_name = _get_span_name("receive", record.topic)
    with tracer.start_as_current_span(
        span_name,
        context=extracted_context,
        kind=trace.SpanKind.CONSUMER,
    ) as span:
        new_context = trace.set_span_in_context(span, extracted_context)
        token = context.attach(new_context)
        _enrich_getone_span(
            span,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            consumer_group=consumer_group,
            topic=record.topic,
            partition=record.partition,
            key=_deserialize_key(record.key),
            offset=record.offset,
        )
        try:
            if async_consume_hook is not None:
                await async_consume_hook(span, record, args, kwargs)
        except Exception as hook_exception:  # pylint: disable=W0703
            _LOG.exception(hook_exception)
        if token:
            context.detach(token)

    return span


def _wrap_getone(  # type: ignore[reportUnusedFunction]
    tracer: Tracer, async_consume_hook: ConsumeHookT | None
) -> Callable[..., Awaitable[aiokafka.ConsumerRecord[object, object]]]:
    async def _traced_getone(
        func: AIOKafkaGetOneProto,
        instance: aiokafka.AIOKafkaConsumer,
        args: tuple[aiokafka.TopicPartition, ...],
        kwargs: dict[str, Any],
    ) -> aiokafka.ConsumerRecord[object, object]:
        record = await func(*args, **kwargs)

        if record:
            bootstrap_servers = _extract_bootstrap_servers(instance._client)
            client_id = _extract_client_id(instance._client)
            consumer_group = _extract_consumer_group(instance)

            extracted_context = propagate.extract(
                record.headers, getter=_aiokafka_getter
            )
            await _create_consumer_span(
                tracer,
                async_consume_hook,
                record,
                extracted_context,
                bootstrap_servers,
                client_id,
                consumer_group,
                args,
                kwargs,
            )
        return record

    return _traced_getone


def _wrap_getmany(  # type: ignore[reportUnusedFunction]
    tracer: Tracer, async_consume_hook: ConsumeHookT | None
) -> Callable[
    ...,
    Awaitable[
        dict[
            aiokafka.TopicPartition,
            list[aiokafka.ConsumerRecord[object, object]],
        ]
    ],
]:
    async def _traced_getmany(
        func: AIOKafkaGetManyProto,
        instance: aiokafka.AIOKafkaConsumer,
        args: tuple[aiokafka.TopicPartition, ...],
        kwargs: dict[str, Any],
    ) -> dict[
        aiokafka.TopicPartition, list[aiokafka.ConsumerRecord[object, object]]
    ]:
        records = await func(*args, **kwargs)

        if records:
            bootstrap_servers = _extract_bootstrap_servers(instance._client)
            client_id = _extract_client_id(instance._client)
            consumer_group = _extract_consumer_group(instance)

            span_name = _get_span_name(
                "receive",
                ", ".join(sorted({topic.topic for topic in records.keys()})),
            )
            with tracer.start_as_current_span(
                span_name, kind=trace.SpanKind.CONSUMER
            ) as poll_span:
                _enrich_getmany_poll_span(
                    poll_span,
                    bootstrap_servers=bootstrap_servers,
                    client_id=client_id,
                    consumer_group=consumer_group,
                    message_count=sum(len(r) for r in records.values()),
                )

                for topic, topic_records in records.items():
                    span_name = _get_span_name("receive", topic.topic)
                    with tracer.start_as_current_span(
                        span_name, kind=trace.SpanKind.CONSUMER
                    ) as topic_span:
                        _enrich_getmany_topic_span(
                            topic_span,
                            bootstrap_servers=bootstrap_servers,
                            client_id=client_id,
                            consumer_group=consumer_group,
                            topic=topic.topic,
                            partition=topic.partition,
                            message_count=len(topic_records),
                        )

                        for record in topic_records:
                            extracted_context = propagate.extract(
                                record.headers, getter=_aiokafka_getter
                            )
                            record_span = await _create_consumer_span(
                                tracer,
                                async_consume_hook,
                                record,
                                extracted_context,
                                bootstrap_servers,
                                client_id,
                                consumer_group,
                                args,
                                kwargs,
                            )
                            topic_span.add_link(record_span.get_span_context())
        return records

    return _traced_getmany
