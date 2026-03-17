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
# subscr.py
#
# Contains the Subscription class and Message classes used for managing
# subscriptions to database events and the messages that are sent when those
# events are detected.
# -----------------------------------------------------------------------------

from typing import Callable, Union, List
from . import connection


class Subscription:
    def __repr__(self):
        return f"<oracledb.Subscription on {self.connection!r}>"

    @classmethod
    def _from_impl(cls, impl):
        subscr = cls.__new__(cls)
        subscr._impl = impl
        return subscr

    @property
    def callback(self) -> Callable:
        """
        Returns the callback that was registered when the subscription was
        created.
        """
        return self._impl.callback

    @property
    def connection(self) -> "connection.Connection":
        """
        Returns the connection that was used to register the subscription when
        it was created.
        """
        return self._impl.connection

    @property
    def id(self) -> int:
        """
        Returns the value of REGID found in the database view
        USER_CHANGE_NOTIFICATION_REGS or the value of REG_ID found in the
        database view USER_SUBSCR_REGISTRATIONS. For AQ subscriptions, this
        value is 0.
        """
        return self._impl.id

    @property
    def ip_address(self) -> str:
        """
        Returns the IP address used for callback notifications from the
        database server. If not set during construction, this value is None.
        """
        return self._impl.ip_address

    @property
    def ipAddress(self) -> str:
        """
        Deprecated. Use property ip_address instead.
        """
        return self.ip_address

    @property
    def name(self) -> str:
        """
        Returns the name used to register the subscription when it was created.
        """
        return self._impl.name

    @property
    def namespace(self) -> int:
        """
        Returns the namespace used to register the subscription when it was
        created.
        """
        return self._impl.namespace

    @property
    def operations(self) -> int:
        """
        Returns the operations that will send notifications for each table or
        query that is registered using this subscription.
        """
        return self._impl.operations

    @property
    def port(self) -> int:
        """
        Returns the port used for callback notifications from the database
        server. If not set during construction, this value is zero.
        """
        return self._impl.port

    @property
    def protocol(self) -> int:
        """
        Returns the protocol used to register the subscription when it was
        created.
        """
        return self._impl.protocol

    @property
    def qos(self) -> int:
        """
        Returns the quality of service flags used to register the subscription
        when it was created.
        """
        return self._impl.qos

    def registerquery(
        self, statement: str, args: Union[list, dict] = None
    ) -> int:
        """
        Register the query for subsequent notification when tables referenced
        by the query are changed. This behaves similarly to cursor.execute()
        but only queries are permitted and the args parameter, if specified,
        must be a sequence or dictionary. If the qos parameter included the
        flag SUBSCR_QOS_QUERY when the subscription was created, then the ID
        for the registered query is returned; otherwise, None is returned.
        """
        if args is not None and not isinstance(args, (list, dict)):
            raise TypeError("expecting args to be a dictionary or list")
        return self._impl.register_query(statement, args)

    @property
    def timeout(self) -> int:
        """
        Returns the timeout (in seconds) that was specified when the
        subscription was created. A value of 0 indicates that there is no
        timeout.
        """
        return self._impl.timeout


class Message:
    def __init__(self, subscription: Subscription) -> None:
        self._subscription = subscription
        self._consumer_name = None
        self._dbname = None
        self._queries = []
        self._queue_name = None
        self._registered = False
        self._tables = []
        self._txid = None
        self._type = 0
        self._msgid = None

    @property
    def consumer_name(self) -> Union[str, None]:
        """
        Returns the name of the consumer which generated the notification. It
        will be populated if the subscription was created with the namespace
        SUBSCR_NAMESPACE_AQ and the queue is a multiple consumer queue.
        """
        return self._consumer_name

    @property
    def consumerName(self) -> Union[str, None]:
        """
        Deprecated. Use property consumer_name instead.
        """
        return self.consumer_name

    @property
    def dbname(self) -> Union[str, None]:
        """
        Returns the name of the database that generated the notification.
        """
        return self._dbname

    @property
    def msgid(self) -> Union[bytes, None]:
        """
        Returns the message id of the AQ message that generated the
        notification.
        """
        return self._msgid

    @property
    def queries(self) -> List["MessageQuery"]:
        """
        Returns a list of message query objects that give information about
        query result sets changed for this notification. This attribute will be
        an empty list if the qos parameter did not include the flag
        SUBSCR_QOS_QUERY when the subscription was created.
        """
        return self._queries

    @property
    def queue_name(self) -> Union[str, None]:
        """
        Returns the name of the queue which generated the notification. It will
        only be populated if the subscription was created with the namespace
        SUBSCR_NAMESPACE_AQ.
        """
        return self._queue_name

    @property
    def queueName(self) -> Union[str, None]:
        """
        Deprecated. Use property queue_name instead.
        """
        return self.queue_name

    @property
    def registered(self) -> bool:
        """
        Returns whether the subscription which generated this notification is
        still registered with the database. The subscription is automatically
        deregistered with the database when the subscription timeout value is
        reached or when the first notification is sent (when the quality of
        service flag SUBSCR_QOS_DEREG_NFY is used).
        """
        return self._registered

    @property
    def subscription(self) -> Subscription:
        """
        Returns the subscription object for which this notification was
        generated.
        """
        return self._subscription

    @property
    def tables(self) -> List["MessageTable"]:
        """
        Returns a list of message table objects that give information about the
        tables changed for this notification. This attribute will be an empty
        list if the qos parameter included the flag SUBSCR_QOS_QUERY when the
        subscription was created.
        """
        return self._tables

    @property
    def txid(self) -> Union[bytes, None]:
        """
        Returns the id of the transaction that generated the notification.
        """
        return self._txid

    @property
    def type(self) -> int:
        """
        Returns the type of message that has been sent.
        """
        return self._type


class MessageQuery:
    def __init__(self) -> None:
        self._id = 0
        self._operation = 0
        self._tables = []

    @property
    def id(self) -> int:
        """
        Returns the query id of the query for which the result set changed. The
        value will match the value returned by Subscription.registerquery()
        when the related query was registered.
        """
        return self._id

    @property
    def operation(self) -> int:
        """
        Returns the operation that took place on the query result set that was
        changed. Valid values for this attribute are EVENT_DEREG and
        EVENT_QUERYCHANGE.
        """
        return self._operation

    @property
    def tables(self) -> List["MessageTable"]:
        """
        Returns a list of message table objects that give information about the
        table changes that caused the query result set to change for this
        notification.
        """
        return self._tables


class MessageRow:
    def __init__(self) -> None:
        self._operation = 0
        self._rowid = None

    @property
    def operation(self) -> int:
        """
        Returns the operation that took place on the row that was changed.
        """
        return self._operation

    @property
    def rowid(self) -> Union[str, None]:
        """
        Returns the rowid of the row that was changed.
        """
        return self._rowid


class MessageTable:
    def __init__(self) -> None:
        self._name = None
        self._operation = 0
        self._rows = []

    @property
    def name(self) -> Union[str, None]:
        """
        Returns the name of the table that was changed.
        """
        return self._name

    @property
    def operation(self) -> int:
        """
        Returns the operation that took place on the table that was changed.
        """
        return self._operation

    @property
    def rows(self) -> List["MessageRow"]:
        """
        Returns a list of message row objects that give information about the
        rows changed on the table. This value is only filled in if the qos
        parameter to the Connection.subscribe() method included the flag
        SUBSCR_QOS_ROWIDS.
        """
        return self._rows
