# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2023, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# aq.py
#
# Contains the classes used for handling Advanced Queuing (AQ): Queue,
# DeqOptions, EnqOptions and MessageProperties.
# -----------------------------------------------------------------------------

import datetime

from . import connection as connection_module
from typing import Any, Union, List
from . import errors
from .dbobject import DbObject, DbObjectType


class Queue:
    @classmethod
    def _from_impl(cls, connection, impl):
        queue = cls.__new__(cls)
        queue._connection = connection
        queue._deq_options = DeqOptions._from_impl(impl.deq_options_impl)
        queue._enq_options = EnqOptions._from_impl(impl.enq_options_impl)
        queue._payload_type = None
        queue._impl = impl
        return queue

    def _verify_message(self, message: "MessageProperties") -> None:
        """
        Internal method used for verifying a message.
        """
        if not isinstance(message, MessageProperties):
            raise TypeError("expecting MessageProperties object")
        if message.payload is None:
            errors._raise_err(errors.ERR_MESSAGE_HAS_NO_PAYLOAD)

    @property
    def connection(self) -> "connection_module.Connection":
        """
        Returns the connection on which the queue was created.
        """
        return self._connection

    def deqmany(self, max_num_messages: int) -> list:
        """
        Dequeues up to the specified number of messages from the queue and
        returns a list of these messages.
        """
        message_impls = self._impl.deq_many(max_num_messages)
        return [MessageProperties._from_impl(impl) for impl in message_impls]

    def deqMany(self, max_num_messages: int) -> List["MessageProperties"]:
        """
        Deprecated: use deqmany() instead.
        """
        return self.deqmany(max_num_messages)

    def deqone(self) -> Union["MessageProperties", None]:
        """
        Dequeues at most one message from the queue and returns it. If no
        message is dequeued, None is returned.
        """
        message_impl = self._impl.deq_one()
        if message_impl is not None:
            return MessageProperties._from_impl(message_impl)

    def deqOne(self) -> Union["MessageProperties", None]:
        """
        Deprecated: use deqone() instead.
        """
        return self.deqone()

    @property
    def deqoptions(self) -> "DeqOptions":
        """
        Returns the options that will be used when dequeuing messages from the
        queue.
        """
        return self._deq_options

    @property
    def deqOptions(self) -> "DeqOptions":
        """
        Deprecated: use deqoptions instead.
        """
        return self.deqoptions

    def enqmany(self, messages: list) -> None:
        """
        Enqueues multiple messages into the queue. The messages parameter must
        be a sequence containing message property objects which have all had
        their payload attribute set to a value that the queue supports.

        Warning: calling this function in parallel on different connections
        acquired from the same pool may fail due to Oracle bug 29928074. Ensure
        that this function is not run in parallel, use standalone connections
        or connections from different pools, or make multiple calls to
        enqOne() instead. The function Queue.deqMany() call is not affected.
        """
        for message in messages:
            self._verify_message(message)
        message_impls = [m._impl for m in messages]
        self._impl.enq_many(message_impls)

    def enqMany(self, messages: list) -> None:
        """
        Deprecated: use enqmany() instead.
        """
        return self.enqmany(messages)

    def enqone(self, message: "MessageProperties") -> None:
        """
        Enqueues a single message into the queue. The message must be a message
        property object which has had its payload attribute set to a value that
        the queue supports.
        """
        self._verify_message(message)
        self._impl.enq_one(message._impl)

    def enqOne(self, message: "MessageProperties") -> None:
        """
        Deprecated: use enqone() instead.
        """
        return self.enqone(message)

    @property
    def enqoptions(self) -> "EnqOptions":
        """
        Returns the options that will be used when enqueuing messages into the
        queue.
        """
        return self._enq_options

    @property
    def enqOptions(self) -> "EnqOptions":
        """
        Deprecated: use enqoptions() instead.
        """
        return self.enqoptions

    @property
    def name(self) -> str:
        """
        Returns the name of the queue.
        """
        return self._impl.name

    @property
    def payload_type(self) -> Union[DbObjectType, None]:
        """
        Returns the object type for payloads that can be enqueued and dequeued.
        If using a raw queue, this returns the value None.
        """
        if self._payload_type is None:
            if self._impl.is_json:
                self._payload_type = "JSON"
            elif self._impl.payload_type is not None:
                self._payload_type = DbObjectType._from_impl(
                    self._impl.payload_type
                )
        return self._payload_type

    @property
    def payloadType(self) -> Union[DbObjectType, None]:
        """
        Deprecated: use payload_type instead.
        """
        return self.payload_type


class DeqOptions:
    @classmethod
    def _from_impl(cls, impl):
        options = cls.__new__(cls)
        options._impl = impl
        return options

    @property
    def condition(self) -> str:
        """
        Specifies a boolean expression similar to the where clause of a SQL
        query. The boolean expression can include conditions on message
        properties, user data properties and PL/SQL or SQL functions. The
        default is to have no condition specified.
        """
        return self._impl.get_condition()

    @condition.setter
    def condition(self, value: str) -> None:
        self._impl.set_condition(value)

    @property
    def consumername(self) -> str:
        """
        Specifies the name of the consumer. Only messages matching the consumer
        name will be accessed. If the queue is not set up for multiple
        consumers this attribute should not be set. The default is to have no
        consumer name specified.
        """
        return self._impl.get_consumer_name()

    @consumername.setter
    def consumername(self, value: str) -> None:
        self._impl.set_consumer_name(value)

    @property
    def correlation(self) -> str:
        """
        Specifies the correlation identifier of the message to be dequeued.
        Special pattern-matching characters, such as the percent sign (%) and
        the underscore (_), can be used. If multiple messages satisfy the
        pattern, the order of dequeuing is indeterminate. The default is to
        have no correlation specified.
        """
        return self._impl.get_correlation()

    @correlation.setter
    def correlation(self, value: str) -> None:
        self._impl.set_correlation(value)

    @property
    def deliverymode(self) -> None:
        """
        Specifies what types of messages should be dequeued. It should be one
        of the values MSG_PERSISTENT (default), MSG_BUFFERED or
        MSG_PERSISTENT_OR_BUFFERED.
        """
        raise AttributeError("deliverymode can only be written")

    @deliverymode.setter
    def deliverymode(self, value: int) -> None:
        self._impl.set_delivery_mode(value)

    @property
    def mode(self) -> int:
        """
        Specifies the locking behaviour associated with the dequeue operation.
        It should be one of the values DEQ_BROWSE, DEQ_LOCKED, DEQ_REMOVE
        (default), or DEQ_REMOVE_NODATA.
        """
        return self._impl.get_mode()

    @mode.setter
    def mode(self, value: int) -> None:
        self._impl.set_mode(value)

    @property
    def msgid(self) -> bytes:
        """
        Specifies the identifier of the message to be dequeued. The default is
        to have no message identifier specified.
        """
        return self._impl.get_message_id()

    @msgid.setter
    def msgid(self, value: bytes) -> None:
        self._impl.set_message_id(value)

    @property
    def navigation(self) -> int:
        """
        Specifies the position of the message that is retrieved. It should be
        one of the values DEQ_FIRST_MSG, DEQ_NEXT_MSG (default), or
        DEQ_NEXT_TRANSACTION.
        """
        return self._impl.get_navigation()

    @navigation.setter
    def navigation(self, value: int) -> None:
        self._impl.set_navigation(value)

    @property
    def transformation(self) -> str:
        """
        Specifies the name of the transformation that must be applied after the
        message is dequeued from the database but before it is returned to the
        calling application. The transformation must be created using
        dbms_transform. The default is to have no transformation specified.
        """
        return self._impl.get_transformation()

    @transformation.setter
    def transformation(self, value: str) -> None:
        self._impl.set_transformation(value)

    @property
    def visibility(self) -> int:
        """
        Specifies the transactional behavior of the dequeue request. It should
        be one of the values DEQ_ON_COMMIT (default) or DEQ_IMMEDIATE. This
        attribute is ignored when using the DEQ_BROWSE mode. Note the value of
        autocommit is always ignored.
        """
        return self._impl.get_visibility()

    @visibility.setter
    def visibility(self, value: int) -> None:
        self._impl.set_visibility(value)

    @property
    def wait(self) -> int:
        """
        Specifies the time to wait, in seconds, for a message matching the
        search criteria to become available for dequeuing. One of the values
        DEQ_NO_WAIT or DEQ_WAIT_FOREVER can also be used. The default is
        DEQ_WAIT_FOREVER.
        """
        return self._impl.get_wait()

    @wait.setter
    def wait(self, value: int) -> None:
        self._impl.set_wait(value)


class EnqOptions:
    @classmethod
    def _from_impl(cls, impl):
        options = cls.__new__(cls)
        options._impl = impl
        return options

    @property
    def deliverymode(self) -> int:
        """
        Specifies what type of messages should be enqueued. It should be one of
        the values MSG_PERSISTENT (default) or MSG_BUFFERED.
        """
        raise AttributeError("deliverymode can only be written")

    @deliverymode.setter
    def deliverymode(self, value: int) -> None:
        self._impl.set_delivery_mode(value)

    @property
    def transformation(self) -> str:
        """
        Specifies the name of the transformation that must be applied before
        the message is enqueued into the database. The transformation must be
        created using dbms_transform. The default is to have no transformation
        specified.
        """
        return self._impl.get_transformation()

    @transformation.setter
    def transformation(self, value: str) -> None:
        self._impl.set_transformation(value)

    @property
    def visibility(self) -> int:
        """
        Specifies the transactional behavior of the enqueue request. It should
        be one of the values ENQ_ON_COMMIT (default) or ENQ_IMMEDIATE. Note the
        value of autocommit is ignored.
        """
        return self._impl.get_visibility()

    @visibility.setter
    def visibility(self, value: int) -> None:
        self._impl.set_visibility(value)


class MessageProperties:
    _recipients = []

    @classmethod
    def _from_impl(cls, impl):
        props = cls.__new__(cls)
        props._impl = impl
        return props

    @property
    def attempts(self) -> int:
        """
        Specifies the number of attempts that have been made to dequeue the
        message.
        """
        return self._impl.get_num_attempts()

    @property
    def correlation(self) -> str:
        """
        Specifies the correlation used when the message was enqueued.
        """
        return self._impl.get_correlation()

    @correlation.setter
    def correlation(self, value: str) -> None:
        self._impl.set_correlation(value)

    @property
    def delay(self) -> int:
        """
        Specifies the number of seconds to delay an enqueued message. Any
        integer is acceptable but the constant MSG_NO_DELAY can also be used
        indicating that the message is available for immediate dequeuing.
        """
        return self._impl.get_delay()

    @delay.setter
    def delay(self, value: int) -> None:
        self._impl.set_delay(value)

    @property
    def deliverymode(self) -> int:
        """
        Specifies the type of message that was dequeued. It will be one of the
        values MSG_PERSISTENT or MSG_BUFFERED.
        """
        return self._impl.get_delivery_mode()

    @property
    def enqtime(self) -> datetime.datetime:
        """
        Specifies the time that the message was enqueued.
        """
        return self._impl.get_enq_time()

    @property
    def exceptionq(self) -> str:
        """
        Specifies the name of the queue to which the message is moved if it
        cannot be processed successfully. Messages are moved if the number of
        unsuccessful dequeue attempts has exceeded the maximum number of
        retries or if the message has expired. All messages in the exception
        queue are in the MSG_EXPIRED state. The default value is the name of
        the exception queue associated with the queue table.
        """
        return self._impl.get_exception_queue()

    @exceptionq.setter
    def exceptionq(self, value: str) -> None:
        self._impl.set_exception_queue(value)

    @property
    def expiration(self) -> int:
        """
        Specifies, in seconds, how long the message is available for dequeuing.
        This attribute is an offset from the delay attribute. Expiration
        processing requires the queue monitor to be running. Any integer is
        accepted but the constant MSG_NO_EXPIRATION can also be used indicating
        that the message never expires.
        """
        return self._impl.get_expiration()

    @expiration.setter
    def expiration(self, value: int) -> None:
        self._impl.set_expiration(value)

    @property
    def msgid(self) -> bytes:
        """
        Specifies the id of the message in the last queue that enqueued or
        dequeued this message. If the message has never been dequeued or
        enqueued, the value will be `None`.
        """
        return self._impl.get_message_id()

    @property
    def payload(self) -> Union[bytes, DbObject]:
        """
        Specifies the payload that will be enqueued or the payload that was
        dequeued when using a queue. When enqueuing, the value is checked to
        ensure that it conforms to the type expected by that queue. For RAW
        queues, the value can be a bytes object or a string. If the value is a
        string it will be converted to bytes in the encoding UTF-8.
        """
        return self._impl.payload

    @payload.setter
    def payload(self, value: Any) -> None:
        if isinstance(value, DbObject):
            self._impl.set_payload_object(value._impl)
        elif not isinstance(value, (str, bytes)):
            self._impl.set_payload_json(value)
        else:
            if isinstance(value, str):
                value_bytes = value.encode()
            elif isinstance(value, bytes):
                value_bytes = value
            self._impl.set_payload_bytes(value_bytes)
        self._impl.payload = value

    @property
    def priority(self) -> int:
        """
        Specifies the priority of the message. A smaller number indicates a
        higher priority. The priority can be any integer, including negative
        numbers. The default value is zero.
        """
        return self._impl.get_priority()

    @priority.setter
    def priority(self, value: int) -> None:
        self._impl.set_priority(value)

    @property
    def recipients(self) -> list:
        """
        A list of recipient names can be associated with a message at the time
        a message is enqueued. This allows a limited set of recipients to
        dequeue each message. The recipient list associated with the message
        overrides the queue subscriber list, if there is one. The recipient
        names need not be in the subscriber list but can be, if desired.

        To dequeue a message, the consumername attribute can be set to one of
        the recipient names. The original message recipient list is not
        available on dequeued messages. All recipients have to dequeue a
        message before it gets removed from the queue.

        Subscribing to a queue is like subscribing to a magazine: each
        subscriber can dequeue all the messages placed into a specific queue,
        just as each magazine subscriber has access to all its articles. Being
        a recipient, however, is like getting a letter: each recipient is a
        designated target of a particular message.
        """
        return self._recipients

    @recipients.setter
    def recipients(self, value: list) -> None:
        self._impl.set_recipients(value)
        self._recipients = value

    @property
    def state(self) -> int:
        """
        Specifies the state of the message at the time of the dequeue. It will
        be one of the values MSG_WAITING, MSG_READY, MSG_PROCESSED or
        MSG_EXPIRED.
        """
        return self._impl.get_state()
