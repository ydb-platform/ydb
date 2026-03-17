import asyncio
import dataclasses

import aio_pika


class BaseError(Exception):
    pass


class RabbitMqDisabledError(BaseError):
    pass


@dataclasses.dataclass(frozen=True)
class ConnectionInfo:
    """RabbitMQ connection parameters"""

    host: str
    tcp_port: int


class Channel:
    def __init__(self, channel: aio_pika.Channel):
        self._channel = channel

    async def __aenter__(self) -> 'Channel':
        if not self._channel.is_initialized:
            await self._channel.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._channel.close(exc_val)

    async def declare_exchange(
        self,
        exchange: str,
        exchange_type: aio_pika.ExchangeType,
        timeout: float = 1.0,
    ) -> None:
        await self._channel.declare_exchange(
            name=exchange,
            type=exchange_type,
            timeout=timeout,
        )

    async def declare_queue(self, queue: str, timeout: float = 1.0) -> None:
        await self._channel.declare_queue(name=queue, timeout=timeout)

    async def bind_queue(
        self,
        exchange: str,
        queue: str,
        routing_key: str,
        timeout: float = 1.0,
    ):
        async def _do_bind():
            rmq_queue = await self._channel.get_queue(queue)
            await rmq_queue.bind(exchange=exchange, routing_key=routing_key)

        await asyncio.wait_for(_do_bind(), timeout=timeout)

    async def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        timeout: float = 1.0,
    ):
        async def _do_publish():
            rmq_exchange = await self._channel.get_exchange(name=exchange)
            await rmq_exchange.publish(
                aio_pika.Message(body=body),
                routing_key=routing_key,
            )

        await asyncio.wait_for(_do_publish(), timeout=timeout)

    async def consume(self, queue: str, count: int, timeout: float = 2.0):
        async def _do_consume():
            result = []

            rmq_queue = await self._channel.get_queue(name=queue)

            for i in range(count):
                incoming_message = await rmq_queue.get()
                if incoming_message is not None:
                    await incoming_message.ack()
                    result.append(
                        incoming_message.body[: incoming_message.body_size],
                    )

            return result

        return await asyncio.wait_for(_do_consume(), timeout=timeout)


class Client:
    def __init__(self, connection_future):
        self._connection_future = connection_future
        self._connection = None

    async def teardown(self):
        if self._connection is not None:
            await self._connection.close()

    async def get_channel(self) -> Channel:
        if self._connection is None:
            self._connection = await self._connection_future
        return Channel(
            channel=self._connection.channel(publisher_confirms=True),
        )


class Control:
    def __init__(self, enabled: bool, conn_info: ConnectionInfo):
        self._enabled = enabled
        if self._enabled:
            self._client = Client(
                connection_future=aio_pika.connect_robust(
                    host=conn_info.host,
                    port=conn_info.tcp_port,
                    timeout=2.0,
                ),
            )

    async def teardown(self):
        if self._enabled:
            await self._client.teardown()

    async def get_channel(self) -> Channel:
        if not self._enabled:
            raise RabbitMqDisabledError

        return await self._client.get_channel()
