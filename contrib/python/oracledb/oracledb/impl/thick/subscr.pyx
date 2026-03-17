#------------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# subscr.pyx
#
# Cython file defining the thick Subscription implementation class (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------

cdef int _callback_handler(void* context,
                           dpiSubscrMessage* message) except -1 with gil:
    cdef:
        object subscr = <object> context
        ThickSubscrImpl subscr_impl
        object py_message
    if message.errorInfo:
        _raise_from_info(message.errorInfo)
    else:
        subscr_impl = subscr._impl
        py_message = PY_TYPE_MESSAGE(subscr)
        subscr_impl._populate_message(message, py_message)
        subscr.callback(py_message)


cdef class ThickSubscrImpl(BaseSubscrImpl):
    cdef:
        dpiSubscr* _handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiSubscr_release(self._handle)

    cdef object _create_message_query(self, dpiSubscrMessageQuery* query):
        cdef:
            object py_query = PY_TYPE_MESSAGE_QUERY()
            uint32_t i
            list temp
        py_query._id = query.id
        py_query._operation = query.operation
        temp = py_query._tables
        for i in range(query.numTables):
            temp.append(self._create_message_table(&query.tables[i]))
        return py_query

    cdef object _create_message_row(self, dpiSubscrMessageRow* row):
        cdef object py_row = PY_TYPE_MESSAGE_ROW()
        py_row._operation = row.operation
        py_row._rowid = row.rowid[:row.rowidLength].decode()
        return py_row

    cdef object _create_message_table(self, dpiSubscrMessageTable* table):
        cdef:
            object py_table = PY_TYPE_MESSAGE_TABLE()
            uint32_t i
            list temp
        py_table._operation = table.operation
        py_table._name = table.name[:table.nameLength].decode()
        temp = py_table._rows
        for i in range(table.numRows):
            temp.append(self._create_message_row(&table.rows[i]))
        return py_table

    cdef object _populate_message(self, dpiSubscrMessage* message,
                                  object py_message):
        cdef:
            const char* txid
            list temp
            uint32_t i
        py_message._type = message.eventType
        py_message._registered = message.registered
        py_message._dbname = message.dbName[:message.dbNameLength].decode()
        if message.txId != NULL:
            txid = <const char*> message.txId
            py_message._txid = txid[:message.txIdLength]
        if message.queueName != NULL:
            py_message._queue_name = \
                    message.queueName[:message.queueNameLength].decode()
        if message.consumerName != NULL:
            py_message._consumer_name = \
                    message.consumerName[:message.consumerNameLength].decode()
        if message.aqMsgId != NULL:
            msgid = <const char*> message.aqMsgId
            py_message._msgid = msgid[:message.aqMsgIdLength]
        if message.eventType == DPI_EVENT_OBJCHANGE:
            temp = py_message._tables
            for i in range(message.numTables):
                temp.append(self._create_message_table(&message.tables[i]))
        elif message.eventType == DPI_EVENT_QUERYCHANGE:
            temp = py_message._queries
            for i in range(message.numQueries):
                temp.append(self._create_message_query(&message.queries[i]))

    def register_query(self, str sql, object args):
        """
        Internal method for registering a query.
        """
        cdef:
            StringBuffer sql_buf = StringBuffer()
            ThickCursorImpl cursor_impl
            uint64_t query_id
            object cursor
        sql_buf.set_value(sql)
        cursor = self.connection.cursor()
        cursor_impl = <ThickCursorImpl> cursor._impl
        if dpiSubscr_prepareStmt(self._handle, sql_buf.ptr, sql_buf.length,
                                 &cursor_impl._handle) < 0:
            _raise_from_odpi()
        if args is not None:
            cursor_impl.bind_one(cursor, args)
        cursor_impl.execute(cursor)
        if self.qos & DPI_SUBSCR_QOS_QUERY:
            if dpiStmt_getSubscrQueryId(cursor_impl._handle, &query_id) < 0:
                _raise_from_odpi()
            return query_id

    def subscribe(self, object subscr, ThickConnImpl conn_impl):
        """
        Internal method for creating the subscription.
        """
        cdef:
            StringBuffer ip_address_buf = StringBuffer()
            StringBuffer name_buf = StringBuffer()
            dpiSubscrCreateParams params
            int status
        name_buf.set_value(self.name)
        ip_address_buf.set_value(self.ip_address)
        if dpiContext_initSubscrCreateParams(driver_info.context, &params) < 0:
            _raise_from_odpi()
        params.subscrNamespace = self.namespace
        params.protocol = self.protocol
        params.qos = self.qos
        params.operations = self.operations
        params.portNumber = self.port
        params.timeout = self.timeout
        params.name = name_buf.ptr
        params.nameLength = name_buf.length
        if self.callback is not None:
            params.callback = <dpiSubscrCallback> _callback_handler
            params.callbackContext = <void*> subscr
        params.ipAddress = ip_address_buf.ptr
        params.ipAddressLength = ip_address_buf.length
        params.groupingClass = self.grouping_class
        params.groupingType = self.grouping_type
        params.groupingValue = self.grouping_value
        params.clientInitiated = self.client_initiated
        with nogil:
            status = dpiConn_subscribe(conn_impl._handle, &params,
                                       &self._handle)
        if status < 0:
            _raise_from_odpi()
        self.id = params.outRegId

    def unsubscribe(self, ThickConnImpl conn_impl):
        """
        Internal method for destroying the subscription.
        """
        cdef int status
        with nogil:
            status = dpiConn_unsubscribe(conn_impl._handle, self._handle)
        if status < 0:
            _raise_from_odpi()
