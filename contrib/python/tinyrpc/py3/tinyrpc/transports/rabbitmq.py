#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Tuple, Any

import pika

from . import ServerTransport, ClientTransport


class RabbitMQServerTransport(ServerTransport):
    """Server transport based on a :py:class:`pika.BlockingConnection`.

    The transport assumes a RabbitMQ topology has already been established.

    :param connection: A :py:class:`pika.BlockingConnection` instance.
    :param queue: The RabbitMQ queue to consume messages from.
    :param exchange: The RabbitMQ exchange to use.
    """

    def __init__(self, connection: pika.BlockingConnection, queue: str, exchange: str = '') -> None:
        self.connection = connection
        self.queue = queue
        self.exchange = exchange

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.on_receive)
        self.message_received = False

    def receive_message(self) -> Tuple[Any, bytes]:
        while not self.message_received:
            self.connection.process_data_events()
        return self.context, self.message

    def send_reply(self, context: Any, reply: bytes) -> None:
        ch, method, props = context
        ch.basic_publish(exchange=self.exchange,
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = props.correlation_id),
                     body=reply)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.message_received = False # message processed, reset status

    def on_receive(self, ch, method, props, body):
        self.context = (ch, method, props)
        self.message = body
        self.message_received = True

    @classmethod
    def create(cls, host: str, queue: str, exchange: str = '') -> 'RabbitMQServerTransport':
        """Create new server transport.

        Instead of creating the BlockingConnection yourself, you can call this function and
        pass in the host name, queue, and exchange.

        :param host: The host clients will connect to.
        :param queue: The RabbitMQ queue to consume messages from.
        :param exchange: The RabbitMQ exchange to use.
        """
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        return cls(connection, queue, exchange)


class RabbitMQClientTransport(ClientTransport):
    """Client transport based on a :py:class:`pika.BlockingConnection`.

    The transport assumes a RabbitMQ topology has already been established.

    :param connection: A :py:class:`pika.BlockingConnection` instance.
    :param routing_key: The RabbitMQ routing key to direct messages.
    :param exchange: The RabbitMQ exchange to use.
    """

    def __init__(self, connection: pika.BlockingConnection, routing_key: str, exchange: str = '') -> None:
        self.connection = connection
        self.routing_key = routing_key
        self.exchange = exchange
        self._id_counter = 1000

        self.channel = self.connection.channel()
        qd_result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = qd_result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def _get_unique_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    def send_message(self, message: bytes, expect_reply: bool = True) -> bytes:
        self.response_data = None
        self.corr_id = str(self._get_unique_id())
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=message)

        if expect_reply:
            while self.response_data is None:
                self.connection.process_data_events()
            return self.response_data

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response_data = body

    @classmethod
    def create(cls, host: str, routing_key: str, exchange: str = '') -> 'RabbitMQClientTransport':
        """Create new client transport.

        Instead of creating the BlockingConnection yourself, you can call this function and
        pass in the host name, routing key, and exchange.

        :param host: The host clients will connect to.
        :param routing_key: The RabbitMQ routing key to direct messages.
        :param exchange: The RabbitMQ exchange to use.
        """
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        return cls(connection, routing_key, exchange)
