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

from confluent_kafka.cimpl import Consumer as _ConsumerImpl
from .error import (ConsumeError,
                    KeyDeserializationError,
                    ValueDeserializationError)
from .serialization import (SerializationContext,
                            MessageField)


class DeserializingConsumer(_ConsumerImpl):
    """
    A high level Kafka consumer with deserialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library. To avoid breaking changes on upgrading, we
    recommend using deserializers directly.`

    Derived from the :py:class:`Consumer` class, overriding the :py:func:`Consumer.poll`
    method to add deserialization capabilities.

    Additional configuration properties:

    +-------------------------+---------------------+-----------------------------------------------------+
    | Property Name           | Type                | Description                                         |
    +=========================+=====================+=====================================================+
    |                         |                     | Callable(bytes, SerializationContext) -> obj        |
    | ``key.deserializer``    | callable            |                                                     |
    |                         |                     | Deserializer used for message keys.                 |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(bytes, SerializationContext) -> obj        |
    | ``value.deserializer``  | callable            |                                                     |
    |                         |                     | Deserializer used for message values.               |
    +-------------------------+---------------------+-----------------------------------------------------+

    Deserializers for string, integer and double (:py:class:`StringDeserializer`, :py:class:`IntegerDeserializer`
    and :py:class:`DoubleDeserializer`) are supplied out-of-the-box in the ``confluent_kafka.serialization``
    namespace.

    Deserializers for Protobuf, JSON Schema and Avro (:py:class:`ProtobufDeserializer`, :py:class:`JSONDeserializer`
    and :py:class:`AvroDeserializer`) with Confluent Schema Registry integration are supplied out-of-the-box
    in the ``confluent_kafka.schema_registry`` namespace.

    See Also:
        - The :ref:`Configuration Guide <pythonclient_configuration>` for in depth information on how to configure the client.
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for a comprehensive set of configuration properties.
        - `STATISTICS.md <https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md>`_ for detailed information on the statistics provided by stats_cb
        - The :py:class:`Consumer` class for inherited methods.

    Args:
        conf (dict): DeserializingConsumer configuration.

    Raises:
        ValueError: if configuration validation fails
    """  # noqa: E501

    def __init__(self, conf):
        conf_copy = conf.copy()
        self._key_deserializer = conf_copy.pop('key.deserializer', None)
        self._value_deserializer = conf_copy.pop('value.deserializer', None)

        super(DeserializingConsumer, self).__init__(conf_copy)

    def poll(self, timeout=-1):
        """
        Consume messages and calls callbacks.

        Args:
            timeout (float): Maximum time to block waiting for message(Seconds).

        Returns:
            :py:class:`Message` or None on timeout

        Raises:
            KeyDeserializationError: If an error occurs during key deserialization.

            ValueDeserializationError: If an error occurs during value deserialization.

            ConsumeError: If an error was encountered while polling.
        """

        msg = super(DeserializingConsumer, self).poll(timeout)

        if msg is None:
            return None

        if msg.error() is not None:
            raise ConsumeError(msg.error(), kafka_message=msg)

        ctx = SerializationContext(msg.topic(), MessageField.VALUE, msg.headers())
        value = msg.value()
        if self._value_deserializer is not None:
            try:
                value = self._value_deserializer(value, ctx)
            except Exception as se:
                raise ValueDeserializationError(exception=se, kafka_message=msg)

        key = msg.key()
        ctx.field = MessageField.KEY
        if self._key_deserializer is not None:
            try:
                key = self._key_deserializer(key, ctx)
            except Exception as se:
                raise KeyDeserializationError(exception=se, kafka_message=msg)

        msg.set_key(key)
        msg.set_value(value)
        return msg

    def consume(self, num_messages=1, timeout=-1):
        """
        :py:func:`Consumer.consume` not implemented, use
        :py:func:`DeserializingConsumer.poll` instead
        """

        raise NotImplementedError
