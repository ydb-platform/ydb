#!/usr/bin/env python
#
# Copyright 2016-2020 Confluent Inc.
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

"""
    Avro schema registry module: Deals with encoding and decoding of messages with avro schemas
"""

import warnings

from confluent_kafka import Producer, Consumer
from confluent_kafka.avro.error import ClientError
from confluent_kafka.avro.load import load, loads  # noqa
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import (SerializerError,  # noqa
                                             KeySerializerError,
                                             ValueSerializerError)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


class AvroProducer(Producer):
    """
        .. deprecated:: 2.0.2

        This class will be removed in a future version of the library.

        Kafka Producer client which does avro schema encoding to messages.
        Handles schema registration, Message serialization.

        Constructor arguments:

        :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                            and the standard Kafka client configuration (``bootstrap.servers`` et.al).
        :param str default_key_schema: Optional default avro schema for key
        :param str default_value_schema: Optional default avro schema for value
    """

    def __init__(self, config, default_key_schema=None,
                 default_value_schema=None, schema_registry=None, **kwargs):
        warnings.warn(
            "AvroProducer has been deprecated. Use AvroSerializer instead.",
            category=DeprecationWarning, stacklevel=2)

        sr_conf = {key.replace("schema.registry.", ""): value
                   for key, value in config.items() if key.startswith("schema.registry")}

        if sr_conf.get("basic.auth.credentials.source") == 'SASL_INHERIT':
            # Fallback to plural 'mechanisms' for backward compatibility
            sr_conf['sasl.mechanism'] = config.get('sasl.mechanism', config.get('sasl.mechanisms', ''))
            sr_conf['sasl.username'] = config.get('sasl.username', '')
            sr_conf['sasl.password'] = config.get('sasl.password', '')
            sr_conf['auto.register.schemas'] = config.get('auto.register.schemas', True)

        ap_conf = {key: value
                   for key, value in config.items() if not key.startswith("schema.registry")}

        if schema_registry is None:
            schema_registry = CachedSchemaRegistryClient(sr_conf)
        elif sr_conf.get("url", None) is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        super(AvroProducer, self).__init__(ap_conf, **kwargs)
        self._serializer = MessageSerializer(schema_registry)
        self._key_schema = default_key_schema
        self._value_schema = default_value_schema

    def produce(self, **kwargs):
        """
            Asynchronously sends message to Kafka by encoding with specified or default avro schema.

            :param str topic: topic name
            :param object value: An object to serialize
            :param str value_schema: Avro schema for value
            :param object key: An object to serialize
            :param str key_schema: Avro schema for key

            Plus any other parameters accepted by confluent_kafka.Producer.produce

            :raises SerializerError: On serialization failure
            :raises BufferError: If producer queue is full.
            :raises KafkaException: For other produce failures.
        """
        # get schemas from  kwargs if defined
        key_schema = kwargs.pop('key_schema', self._key_schema)
        value_schema = kwargs.pop('value_schema', self._value_schema)
        topic = kwargs.pop('topic', None)
        if not topic:
            raise ClientError("Topic name not specified.")
        value = kwargs.pop('value', None)
        key = kwargs.pop('key', None)

        if value is not None:
            if value_schema:
                value = self._serializer.encode_record_with_schema(topic, value_schema, value)
            else:
                raise ValueSerializerError("Avro schema required for values")

        if key is not None:
            if key_schema:
                key = self._serializer.encode_record_with_schema(topic, key_schema, key, True)
            else:
                raise KeySerializerError("Avro schema required for key")

        super(AvroProducer, self).produce(topic, value, key, **kwargs)


class AvroConsumer(Consumer):
    """
    .. deprecated:: 2.0.2

    This class will be removed in a future version of the library.

    Kafka Consumer client which does avro schema decoding of messages.
    Handles message deserialization.

    Constructor arguments:

    :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                        and the standard Kafka client configuration (``bootstrap.servers`` et.al)
    :param schema reader_key_schema: a reader schema for the message key
    :param schema reader_value_schema: a reader schema for the message value
    :raises ValueError: For invalid configurations
    """

    def __init__(self, config, schema_registry=None, reader_key_schema=None, reader_value_schema=None, **kwargs):
        warnings.warn(
            "AvroConsumer has been deprecated. Use AvroDeserializer instead.",
            category=DeprecationWarning, stacklevel=2)

        sr_conf = {key.replace("schema.registry.", ""): value
                   for key, value in config.items() if key.startswith("schema.registry")}

        if sr_conf.get("basic.auth.credentials.source") == 'SASL_INHERIT':
            # Fallback to plural 'mechanisms' for backward compatibility
            sr_conf['sasl.mechanism'] = config.get('sasl.mechanism', config.get('sasl.mechanisms', ''))
            sr_conf['sasl.username'] = config.get('sasl.username', '')
            sr_conf['sasl.password'] = config.get('sasl.password', '')

        ap_conf = {key: value
                   for key, value in config.items() if not key.startswith("schema.registry")}

        if schema_registry is None:
            schema_registry = CachedSchemaRegistryClient(sr_conf)
        elif sr_conf.get("url", None) is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        super(AvroConsumer, self).__init__(ap_conf, **kwargs)
        self._serializer = MessageSerializer(schema_registry, reader_key_schema, reader_value_schema)

    def poll(self, timeout=None):
        """
        This is an overriden method from confluent_kafka.Consumer class. This handles message
        deserialization using avro schema

        :param float timeout: Poll timeout in seconds (default: indefinite)
        :returns: message object with deserialized key and value as dict objects
        :rtype: Message
        """
        if timeout is None:
            timeout = -1
        message = super(AvroConsumer, self).poll(timeout)
        if message is None:
            return None

        if not message.error():
            try:
                if message.value() is not None:
                    decoded_value = self._serializer.decode_message(message.value(), is_key=False)
                    message.set_value(decoded_value)
                if message.key() is not None:
                    decoded_key = self._serializer.decode_message(message.key(), is_key=True)
                    message.set_key(decoded_key)
            except SerializerError as e:
                raise SerializerError("Message deserialization failed for message at {} [{}] offset {}: {}".format(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    e))
        return message
