#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka.cimpl import Producer as _ProducerImpl
from .serialization import (MessageField,
                            SerializationContext)
from .error import (KeySerializationError,
                    ValueSerializationError)


class SerializingProducer(_ProducerImpl):
    """
    A high level Kafka producer with serialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library. To avoid breaking changes on upgrading, we
    recommend using serializers directly.`

    Derived from the :py:class:`Producer` class, overriding the :py:func:`Producer.produce`
    method to add serialization capabilities.

    Additional configuration properties:

    +-------------------------+---------------------+-----------------------------------------------------+
    | Property Name           | Type                | Description                                         |
    +=========================+=====================+=====================================================+
    |                         |                     | Callable(obj, SerializationContext) -> bytes        |
    | ``key.serializer``      | callable            |                                                     |
    |                         |                     | Serializer used for message keys.                   |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(obj, SerializationContext) -> bytes        |
    | ``value.serializer``    | callable            |                                                     |
    |                         |                     | Serializer used for message values.                 |
    +-------------------------+---------------------+-----------------------------------------------------+

    Serializers for string, integer and double (:py:class:`StringSerializer`, :py:class:`IntegerSerializer`
    and :py:class:`DoubleSerializer`) are supplied out-of-the-box in the ``confluent_kafka.serialization``
    namespace.

    Serializers for Protobuf, JSON Schema and Avro (:py:class:`ProtobufSerializer`, :py:class:`JSONSerializer`
    and :py:class:`AvroSerializer`) with Confluent Schema Registry integration are supplied out-of-the-box
    in the ``confluent_kafka.schema_registry`` namespace.

    See Also:
        - The :ref:`Configuration Guide <pythonclient_configuration>` for in depth information on how to configure the client.
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for a comprehensive set of configuration properties.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information on the statistics provided by stats_cb
        - The :py:class:`Producer` class for inherited methods.

    Args:
        conf (producer): SerializingProducer configuration.
    """  # noqa E501

    def __init__(self, conf):
        conf_copy = conf.copy()

        self._key_serializer = conf_copy.pop('key.serializer', None)
        self._value_serializer = conf_copy.pop('value.serializer', None)

        super(SerializingProducer, self).__init__(conf_copy)

    def produce(self, topic, key=None, value=None, partition=-1,
                on_delivery=None, timestamp=0, headers=None):
        """
        Produce a message.

        This is an asynchronous operation. An application may use the
        ``on_delivery`` argument to pass a function (or lambda) that will be
        called from :py:func:`SerializingProducer.poll` when the message has
        been successfully delivered or permanently fails delivery.

        Note:
            Currently message headers are not supported on the message returned to
            the callback. The ``msg.headers()`` will return None even if the
            original message had headers set.

        Args:
            topic (str): Topic to produce message to.

            key (object, optional): Message payload key.

            value (object, optional): Message payload value.

            partition (int, optional): Partition to produce to, else the
                configured built-in partitioner will be used.

            on_delivery (callable(KafkaError, Message), optional): Delivery
                report callback. Called as a side effect of
                :py:func:`SerializingProducer.poll` or
                :py:func:`SerializingProducer.flush` on successful or
                failed delivery.

            timestamp (float, optional): Message timestamp (CreateTime) in
                milliseconds since Unix epoch UTC (requires broker >= 0.10.0.0).
                Default value is current time.

            headers (dict, optional): Message headers. The header key must be
                a str while the value must be binary, unicode or None. (Requires
                broker version >= 0.11.0.0)

        Raises:
            BufferError: if the internal producer message queue is full.
                (``queue.buffering.max.messages`` exceeded). If this happens
                the application should call :py:func:`SerializingProducer.Poll`
                and try again.

            KeySerializationError: If an error occurs during key serialization.

            ValueSerializationError: If an error occurs during value serialization.

            KafkaException: For all other errors
        """

        ctx = SerializationContext(topic, MessageField.KEY, headers)
        if self._key_serializer is not None:
            try:
                key = self._key_serializer(key, ctx)
            except Exception as se:
                raise KeySerializationError(se)
        ctx.field = MessageField.VALUE
        if self._value_serializer is not None:
            try:
                value = self._value_serializer(value, ctx)
            except Exception as se:
                raise ValueSerializationError(se)

        super(SerializingProducer, self).produce(topic, value, key,
                                                 headers=headers,
                                                 partition=partition,
                                                 timestamp=timestamp,
                                                 on_delivery=on_delivery)
