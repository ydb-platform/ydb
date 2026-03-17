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
from confluent_kafka.cimpl import KafkaException, KafkaError
from confluent_kafka.serialization import SerializationError


class _KafkaClientError(KafkaException):
    """
    Wraps all errors encountered by a Kafka Client

    Args:
        kafka_error (KafkaError): KafkaError instance.

        exception(Exception, optional): The original exception

        kafka_message (Message, optional): The Kafka Message returned
        by the broker.
    """

    def __init__(self, kafka_error, exception=None, kafka_message=None):
        super(_KafkaClientError, self).__init__(kafka_error)
        self.exception = exception
        self.kafka_message = kafka_message

    @property
    def code(self):
        return self.args[0].code()

    @property
    def name(self):
        return self.args[0].name()


class ConsumeError(_KafkaClientError):
    """
    Wraps all errors encountered during the consumption of a message.

    Note:
        In the event of a serialization error the original message
        contents may be retrieved from the ``kafka_message`` attribute.

    Args:
        kafka_error (KafkaError): KafkaError instance.

        exception(Exception, optional): The original exception

        kafka_message (Message, optional): The Kafka Message
        returned by the broker.

    """

    def __init__(self, kafka_error, exception=None, kafka_message=None):
        super(ConsumeError, self).__init__(kafka_error, exception, kafka_message)


class KeyDeserializationError(ConsumeError, SerializationError):
    """
    Wraps all errors encountered during the deserialization of a Kafka
    Message's key.

    Args:
        exception(Exception, optional): The original exception

        kafka_message (Message, optional): The Kafka Message returned
        by the broker.

    """

    def __init__(self, exception=None, kafka_message=None):
        super(KeyDeserializationError, self).__init__(
            KafkaError(KafkaError._KEY_DESERIALIZATION, str(exception)),
            exception=exception, kafka_message=kafka_message)


class ValueDeserializationError(ConsumeError, SerializationError):
    """
    Wraps all errors encountered during the deserialization of a Kafka
    Message's value.

    Args:
        exception(Exception, optional): The original exception

        kafka_message (Message, optional): The Kafka Message returned
        by the broker.

    """

    def __init__(self, exception=None, kafka_message=None):
        super(ValueDeserializationError, self).__init__(
            KafkaError(KafkaError._VALUE_DESERIALIZATION, str(exception)),
            exception=exception, kafka_message=kafka_message)


class ProduceError(_KafkaClientError):
    """
    Wraps all errors encountered when Producing messages.

    Args:
        kafka_error (KafkaError): KafkaError instance.

        exception(Exception, optional): The original exception.
    """

    def __init__(self, kafka_error, exception=None):
        super(ProduceError, self).__init__(kafka_error, exception, None)


class KeySerializationError(ProduceError, SerializationError):
    """
    Wraps all errors encountered during the serialization of a Message key.

    Args:
        exception (Exception): The exception that occurred during serialization.
    """

    def __init__(self, exception=None):
        super(KeySerializationError, self).__init__(
            KafkaError(KafkaError._KEY_SERIALIZATION, str(exception)),
            exception=exception)


class ValueSerializationError(ProduceError, SerializationError):
    """
    Wraps all errors encountered during the serialization of a Message value.

    Args:
        exception (Exception): The exception that occurred during serialization.
    """

    def __init__(self, exception=None):
        super(ValueSerializationError, self).__init__(
            KafkaError(KafkaError._VALUE_SERIALIZATION, str(exception)),
            exception=exception)
