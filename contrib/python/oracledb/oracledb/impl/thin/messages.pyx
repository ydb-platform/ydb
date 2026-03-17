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
# messages.pyx
#
# Cython file defining the various messages that are sent to the database and
# the responses that are received by the client (embedded in thin_impl.pyx).
#------------------------------------------------------------------------------

@cython.freelist(20)
cdef class _OracleErrorInfo:
    cdef:
        uint32_t num
        uint16_t cursor_id
        uint64_t pos
        uint64_t rowcount
        str message
        Rowid rowid
        list batcherrors


cdef class Message:
    cdef:
        BaseThinConnImpl conn_impl
        PipelineOpResultImpl pipeline_result_impl
        _OracleErrorInfo error_info
        uint8_t message_type
        uint8_t function_code
        uint32_t call_status
        uint16_t end_to_end_seq_num
        uint64_t token_num
        bint end_of_response
        bint error_occurred
        bint flush_out_binds
        bint resend
        bint retry
        object warning

    cdef int _check_and_raise_exception(self) except -1:
        """
        Checks to see if an error has occurred. If one has, an error object is
        created and then the appropriate exception raised. Note that if a "dead
        connection" error is detected, the connection is forced closed
        immediately.
        """
        if self.error_occurred:
            error = errors._Error(self.error_info.message,
                                  code=self.error_info.num,
                                  offset=self.error_info.pos)
            if error.is_session_dead:
                self.conn_impl._protocol._force_close()
            raise error.exc_type(error)

    cdef int _initialize(self, BaseThinConnImpl conn_impl) except -1:
        """
        Initializes the message to contain the connection and a place to store
        error information. For DRCP, the status of the connection may change
        after the first round-trip to the database so this information needs to
        be preserved. Child classes may have their own initialization. In order
        to avoid overhead using the constructor, a special hook method is used
        instead.
        """
        conn_impl._protocol._read_buf._check_connected()
        self.conn_impl = conn_impl
        self.message_type = TNS_MSG_TYPE_FUNCTION
        self.error_info = _OracleErrorInfo.__new__(_OracleErrorInfo)
        self._initialize_hook()

    cdef int _initialize_hook(self) except -1:
        """
        A hook that is used by subclasses to perform any necessary
        initialization specific to that class.
        """
        pass

    cdef int _process_error_info(self, ReadBuffer buf) except -1:
        cdef:
            uint32_t num_bytes, i, offset, num_offsets
            _OracleErrorInfo info = self.error_info
            uint16_t temp16, num_errors, error_code
            uint8_t first_byte, flags
            int16_t error_pos
            str error_msg
        buf.read_ub4(&self.call_status)     # end of call status
        buf.skip_ub2()                      # end to end seq#
        buf.skip_ub4()                      # current row number
        buf.skip_ub2()                      # error number
        buf.skip_ub2()                      # array elem error
        buf.skip_ub2()                      # array elem error
        buf.read_ub2(&info.cursor_id)       # cursor id
        buf.read_sb2(&error_pos)            # error position
        buf.skip_ub1()                      # sql type (19c and earlier)
        buf.skip_ub1()                      # fatal?
        buf.skip_ub1()                      # flags
        buf.skip_ub1()                      # user cursor options
        buf.skip_ub1()                      # UPI parameter
        buf.read_ub1(&flags)
        if flags & 0x20:
            self.warning = errors._create_warning(errors.WRN_COMPILATION_ERROR)
        buf.read_rowid(&info.rowid)         # rowid
        buf.skip_ub4()                      # OS error
        buf.skip_ub1()                      # statement number
        buf.skip_ub1()                      # call number
        buf.skip_ub2()                      # padding
        buf.skip_ub4()                      # success iters
        buf.read_ub4(&num_bytes)            # oerrdd (logical rowid)
        if num_bytes > 0:
            buf.skip_raw_bytes_chunked()

        # batch error codes
        buf.read_ub2(&num_errors)           # batch error codes array
        if num_errors > 0:
            info.batcherrors = []
            buf.read_ub1(&first_byte)
            for i in range(num_errors):
                if first_byte == TNS_LONG_LENGTH_INDICATOR:
                    buf.skip_ub4()          # chunk length ignored
                buf.read_ub2(&error_code)
                info.batcherrors.append(errors._Error(code=error_code))
            if first_byte == TNS_LONG_LENGTH_INDICATOR:
                buf.skip_raw_bytes(1)       # ignore end marker

        # batch error offsets
        buf.read_ub4(&num_offsets)          # batch error row offset array
        if num_offsets > 0:
            if num_offsets > 65535:
                errors._raise_err(errors.ERR_TOO_MANY_BATCH_ERRORS)
            buf.read_ub1(&first_byte)
            for i in range(num_offsets):
                if first_byte == TNS_LONG_LENGTH_INDICATOR:
                    buf.skip_ub4()          # chunk length ignored
                buf.read_ub4(&offset)
                if i < num_errors:
                    info.batcherrors[i].offset = offset
            if first_byte == TNS_LONG_LENGTH_INDICATOR:
                buf.skip_raw_bytes(1)       # ignore end marker

        # batch error messages
        buf.read_ub2(&temp16)               # batch error messages array
        if temp16 > 0:
            buf.skip_raw_bytes(1)           # ignore packed size
            for i in range(temp16):
                buf.skip_ub2()              # skip chunk length
                info.batcherrors[i].message = \
                        buf.read_str(CS_FORM_IMPLICIT).rstrip()
                info.batcherrors[i]._make_adjustments()
                buf.skip_raw_bytes(2)       # ignore end marker

        buf.read_ub4(&info.num)             # error number (extended)
        buf.read_ub8(&info.rowcount)        # row number (extended)

        # fields added in Oracle Database 20c
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1:
            buf.skip_ub4()                  # sql type
            buf.skip_ub4()                  # server checksum

        # error message
        if info.num != 0:
            self.error_occurred = True
            if error_pos > 0:
                info.pos = error_pos
            info.message = buf.read_str(CS_FORM_IMPLICIT).rstrip()

        # an error message marks the end of a response if no explicit end of
        # response is available
        if not buf._caps.supports_end_of_response:
            self.end_of_response = True

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        cdef uint64_t token_num
        if message_type == TNS_MSG_TYPE_ERROR:
            self._process_error_info(buf)
        elif message_type == TNS_MSG_TYPE_WARNING:
            self._process_warning_info(buf)
        elif message_type == TNS_MSG_TYPE_TOKEN:
            buf.read_ub8(&token_num)
            if token_num != self.token_num:
                errors._raise_err(errors.ERR_MISMATCHED_TOKEN,
                                  token_num=token_num,
                                  expected_token_num=self.token_num)
        elif message_type == TNS_MSG_TYPE_STATUS:
            buf.read_ub4(&self.call_status)
            buf.read_ub2(&self.end_to_end_seq_num)
            if not buf._caps.supports_end_of_response:
                self.end_of_response = True
        elif message_type == TNS_MSG_TYPE_PARAMETER:
            self._process_return_parameters(buf)
        elif message_type == TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK:
            self._process_server_side_piggyback(buf)
        elif message_type == TNS_MSG_TYPE_END_OF_RESPONSE:
            self.end_of_response = True
        else:
            errors._raise_err(errors.ERR_MESSAGE_TYPE_UNKNOWN,
                              message_type=message_type,
                              position=buf._pos - 1)

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        raise NotImplementedError()

    cdef int _process_server_side_piggyback(self, ReadBuffer buf) except -1:
        cdef:
            uint16_t num_elements, i, temp16
            uint32_t num_bytes, flags
            uint8_t opcode
        buf.read_ub1(&opcode)
        if opcode == TNS_SERVER_PIGGYBACK_LTXID:
            buf.read_ub4(&num_bytes)
            if num_bytes > 0:
                buf.skip_raw_bytes(num_bytes)
        elif opcode == TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION \
                or opcode == TNS_SERVER_PIGGYBACK_TRACE_EVENT:
            pass
        elif opcode == TNS_SERVER_PIGGYBACK_OS_PID_MTS:
            buf.read_ub2(&temp16)
            buf.skip_raw_bytes_chunked()
        elif opcode == TNS_SERVER_PIGGYBACK_SYNC:
            buf.skip_ub2()                  # skip number of DTYs
            buf.skip_ub1()                  # skip length of DTYs
            buf.read_ub2(&num_elements)
            buf.skip_ub1()                  # skip length
            for i in range(num_elements):
                buf.read_ub2(&temp16)
                if temp16 > 0:              # skip key
                    buf.skip_raw_bytes_chunked()
                buf.read_ub2(&temp16)
                if temp16 > 0:              # skip value
                    buf.skip_raw_bytes_chunked()
                buf.skip_ub2()              # skip flags
            buf.skip_ub4()                  # skip overall flags
        elif opcode == TNS_SERVER_PIGGYBACK_EXT_SYNC:
            buf.skip_ub2()                  # skip number of DTYs
            buf.skip_ub1()                  # skip length of DTYs
        elif opcode == TNS_SERVER_PIGGYBACK_AC_REPLAY_CONTEXT:
            buf.skip_ub2()                  # skip number of DTYs
            buf.skip_ub1()                  # skip length of DTYs
            buf.skip_ub4()                  # skip flags
            buf.skip_ub4()                  # skip error code
            buf.skip_ub1()                  # skip queue
            buf.read_ub4(&num_bytes)        # skip replay context
            if num_bytes > 0:
                buf.skip_raw_bytes_chunked()
        elif opcode == TNS_SERVER_PIGGYBACK_SESS_RET:
            buf.skip_ub2()
            buf.skip_ub1()
            buf.read_ub2(&num_elements)
            if num_elements > 0:
                buf.skip_ub1()
                for i in range(num_elements):
                    buf.read_ub2(&temp16)
                    if temp16 > 0:          # skip key
                        buf.skip_raw_bytes_chunked()
                    buf.read_ub2(&temp16)
                    if temp16 > 0:          # skip value
                        buf.skip_raw_bytes_chunked()
                    buf.skip_ub2()          # skip flags
            buf.read_ub4(&flags)            # session flags
            if flags & TNS_SESSGET_SESSION_CHANGED:
                if self.conn_impl._drcp_establish_session:
                    self.conn_impl._statement_cache.clear_open_cursors()
            self.conn_impl._drcp_establish_session = False
            buf.read_ub4(&self.conn_impl._session_id)
            buf.read_ub2(&self.conn_impl._serial_num)
        else:
            errors._raise_err(errors.ERR_UNKNOWN_SERVER_PIGGYBACK,
                              opcode=opcode)

    cdef int _process_warning_info(self, ReadBuffer buf) except -1:
        cdef:
            uint16_t num_bytes, error_num
            str message
        buf.read_ub2(&error_num)            # error number
        buf.read_ub2(&num_bytes)            # length of error message
        buf.skip_ub2()                      # flags
        if error_num != 0 and num_bytes > 0:
            message = buf.read_str(CS_FORM_IMPLICIT).rstrip()
            self.warning = errors._Error(message, code=error_num,
                                         iswarning=True)

    cdef int _write_function_code(self, WriteBuffer buf) except -1:
        buf.write_uint8(self.message_type)
        buf.write_uint8(self.function_code)
        buf.write_seq_num()
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1:
            buf.write_ub8(self.token_num)

    cdef int _write_message(self, WriteBuffer buf) except -1:
        self._write_function_code(buf)

    cdef int _write_piggyback_code(self, WriteBuffer buf,
                                   uint8_t code) except -1:
        buf.write_uint8(TNS_MSG_TYPE_PIGGYBACK)
        buf.write_uint8(code)
        buf.write_seq_num()
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1:
            buf.write_ub8(self.token_num)

    cdef int postprocess(self) except -1:
        pass

    async def postprocess_async(self):
        pass

    cdef int preprocess(self) except -1:
        pass

    cdef int process(self, ReadBuffer buf) except -1:
        cdef uint8_t message_type
        self.end_of_response = False
        self.flush_out_binds = False
        while not self.end_of_response:
            buf.save_point()
            buf.read_ub1(&message_type)
            self._process_message(buf, message_type)

    cdef int send(self, WriteBuffer buf) except -1:
        buf.start_request(TNS_PACKET_TYPE_DATA)
        self._write_message(buf)
        if self.pipeline_result_impl is not None:
            buf._data_flags |= TNS_DATA_FLAGS_END_OF_REQUEST
        buf.end_request()


cdef class MessageWithData(Message):
    cdef:
        BaseThinDbObjectTypeCache type_cache
        BaseThinCursorImpl cursor_impl
        array.array bit_vector_buf
        const char_type *bit_vector
        bint arraydmlrowcounts
        uint32_t row_index
        uint32_t num_execs
        uint16_t num_columns_sent
        list dmlrowcounts
        bint batcherrors
        list out_var_impls
        bint in_fetch
        bint parse_only
        object cursor
        uint32_t offset

    cdef int _adjust_fetch_info(self,
                                ThinVarImpl prev_var_impl,
                                FetchInfoImpl fetch_info) except -1:
        """
        When a query is re-executed but the data type of a column has changed
        the server returns the type information of the new type. However, if
        the data type returned now is a CLOB or BLOB and the data type
        previously returned was CHAR/VARCHAR/RAW (or the equivalent long
        types), then the server returns the data as LONG (RAW), similarly to
        what happens when a define is done to return CLOB/BLOB as string/bytes.
        Detect this situation and adjust the fetch type appropriately.
        """
        cdef:
            FetchInfoImpl prev_fetch_info = prev_var_impl._fetch_info
            uint8_t type_num, csfrm
        if fetch_info.dbtype._ora_type_num == TNS_DATA_TYPE_VARCHAR \
                and prev_fetch_info.dbtype._ora_type_num == TNS_DATA_TYPE_LONG:
            type_num = TNS_DATA_TYPE_LONG
            csfrm = fetch_info.dbtype._csfrm
            fetch_info.dbtype = DbType._from_ora_type_and_csfrm(type_num, csfrm)

        elif fetch_info.dbtype._ora_type_num == TNS_DATA_TYPE_RAW \
                and prev_fetch_info.dbtype._ora_type_num == \
                        TNS_DATA_TYPE_LONG_RAW:
            type_num = TNS_DATA_TYPE_LONG_RAW
            fetch_info.dbtype = DbType._from_ora_type_and_csfrm(type_num, 0)
        elif fetch_info.dbtype._ora_type_num == TNS_DATA_TYPE_CLOB \
                and prev_fetch_info.dbtype._ora_type_num in \
                        (TNS_DATA_TYPE_CHAR, TNS_DATA_TYPE_VARCHAR,
                         TNS_DATA_TYPE_LONG):
            type_num = TNS_DATA_TYPE_LONG
            csfrm = prev_var_impl.dbtype._csfrm
            fetch_info.dbtype = DbType._from_ora_type_and_csfrm(type_num,
                                                                csfrm)
        elif fetch_info.dbtype._ora_type_num == TNS_DATA_TYPE_BLOB \
                and prev_fetch_info.dbtype._ora_type_num in \
                        (TNS_DATA_TYPE_RAW, TNS_DATA_TYPE_LONG_RAW):
            type_num = TNS_DATA_TYPE_LONG_RAW
            fetch_info.dbtype = DbType._from_ora_type_and_csfrm(type_num, 0)

    cdef object _create_cursor_from_describe(self, ReadBuffer buf,
                                             object cursor=None):
        cdef BaseThinCursorImpl cursor_impl
        if cursor is None:
            cursor = self.cursor.connection.cursor()
        cursor_impl = cursor._impl
        cursor_impl._statement = self.conn_impl._get_statement()
        cursor_impl._more_rows_to_fetch = True
        cursor_impl._statement._is_query = True
        self._process_describe_info(buf, cursor, cursor_impl)
        return cursor

    cdef int _get_bit_vector(self, ReadBuffer buf,
                             ssize_t num_bytes) except -1:
        """
        Gets the bit vector from the buffer and stores it for later use by the
        row processing code. Since it is possible that the packet buffer may be
        overwritten by subsequent packet retrieval, the bit vector must be
        copied. An array is stored and a pointer to the underlying memory is
        used for performance reasons.
        """
        cdef const char_type *ptr = buf.read_raw_bytes(num_bytes)
        if self.bit_vector_buf is None:
            self.bit_vector_buf = array.array('B')
            array.resize(self.bit_vector_buf, num_bytes)
        self.bit_vector = <const char_type*> self.bit_vector_buf.data.as_chars
        memcpy(<void*> self.bit_vector, ptr, num_bytes)

    cdef bint _is_duplicate_data(self, uint32_t column_num):
        """
        Returns a boolean indicating if the given column contains data
        duplicated from the previous row. When duplicate data exists, the
        server sends a bit vector. Bits that are set indicate that data is sent
        with the row data; bits that are not set indicate that data should be
        duplicated from the previous row.
        """
        cdef int byte_num, bit_num
        if self.bit_vector == NULL:
            return False
        byte_num = column_num // 8
        bit_num = column_num % 8
        return self.bit_vector[byte_num] & (1 << bit_num) == 0

    cdef int _write_bind_params(self, WriteBuffer buf, list params) except -1:
        cdef:
            bint has_data = False
            list bind_var_impls
            BindInfo bind_info
        bind_var_impls = []
        for bind_info in params:
            if not bind_info._is_return_bind:
                has_data = True
            bind_var_impls.append(bind_info._bind_var_impl)
        self._write_column_metadata(buf, bind_var_impls)

        # write parameter values unless statement contains only returning binds
        if has_data:
            for i in range(self.num_execs):
                buf.write_uint8(TNS_MSG_TYPE_ROW_DATA)
                self._write_bind_params_row(buf, params, i)

    cdef int _preprocess_query(self) except -1:
        """
        Actions that takes place before query data is processed.
        """
        cdef:
            BaseThinCursorImpl cursor_impl = self.cursor_impl
            Statement statement = cursor_impl._statement
            object type_handler, conn
            ThinVarImpl var_impl
            bint uses_fetch_info
            ssize_t i, num_vals

        # set values to indicate the start of a new fetch operation
        self.in_fetch = True
        cursor_impl._more_rows_to_fetch = True
        cursor_impl._buffer_rowcount = cursor_impl._buffer_index = 0
        self.row_index = 0

        # if no fetch variables exist, nothing further to do at this point; the
        # processing that follows will take the metadata returned by the server
        # and use it to create new fetch variables
        if statement._fetch_var_impls is None:
            return 0

        # if the type handler set on the cursor or connection does not match
        # the one that was used during the last fetch, rebuild the fetch
        # variables in order to take the new type handler into account
        conn = self.cursor.connection
        type_handler = cursor_impl._get_output_type_handler(&uses_fetch_info)
        if type_handler is not statement._last_output_type_handler:
            for i, var_impl in enumerate(cursor_impl.fetch_var_impls):
                cursor_impl._create_fetch_var(conn, self.cursor, type_handler,
                                              uses_fetch_info, i,
                                              var_impl._fetch_info)
            statement._last_output_type_handler = type_handler

        # the list of output variables is equivalent to the fetch variables
        self.out_var_impls = cursor_impl.fetch_var_impls

    cdef int _process_bit_vector(self, ReadBuffer buf) except -1:
        cdef ssize_t num_bytes
        buf.read_ub2(&self.num_columns_sent)
        num_bytes = self.cursor_impl._num_columns // 8
        if self.cursor_impl._num_columns % 8 > 0:
            num_bytes += 1
        self._get_bit_vector(buf, num_bytes)

    cdef object _process_column_data(self, ReadBuffer buf,
                                     ThinVarImpl var_impl, uint32_t pos):
        cdef:
            uint8_t num_bytes, ora_type_num, csfrm
            const char* encoding_errors = NULL
            bytes encoding_errors_bytes
            ThinDbObjectTypeImpl typ_impl
            BaseThinCursorImpl cursor_impl
            object column_value = None
            ThinDbObjectImpl obj_impl
            FetchInfoImpl fetch_info
            int32_t actual_num_bytes
            uint32_t buffer_size
            Rowid rowid
        fetch_info = var_impl._fetch_info
        if fetch_info is not None:
            ora_type_num = fetch_info.dbtype._ora_type_num
            csfrm =  fetch_info.dbtype._csfrm
            buffer_size = fetch_info.buffer_size
        else:
            ora_type_num = var_impl.dbtype._ora_type_num
            csfrm = var_impl.dbtype._csfrm
            buffer_size = var_impl.buffer_size
        if var_impl.bypass_decode:
            ora_type_num = TNS_DATA_TYPE_RAW
        if buffer_size == 0 and self.in_fetch \
                and ora_type_num not in (TNS_DATA_TYPE_LONG,
                                         TNS_DATA_TYPE_LONG_RAW,
                                         TNS_DATA_TYPE_UROWID):
            column_value = None             # column is null by describe
        elif ora_type_num == TNS_DATA_TYPE_VARCHAR \
                or ora_type_num == TNS_DATA_TYPE_CHAR \
                or ora_type_num == TNS_DATA_TYPE_LONG:
            if csfrm == CS_FORM_NCHAR:
                buf._caps._check_ncharset_id()
            if var_impl.encoding_errors is not None:
                encoding_errors_bytes = var_impl.encoding_errors.encode()
                encoding_errors = encoding_errors_bytes
            column_value = buf.read_str(csfrm, encoding_errors)
        elif ora_type_num == TNS_DATA_TYPE_RAW \
                or ora_type_num == TNS_DATA_TYPE_LONG_RAW:
            column_value = buf.read_bytes()
        elif ora_type_num == TNS_DATA_TYPE_NUMBER:
            column_value = buf.read_oracle_number(var_impl._preferred_num_type)
        elif ora_type_num == TNS_DATA_TYPE_DATE \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP_LTZ \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP_TZ:
            column_value = buf.read_date()
        elif ora_type_num == TNS_DATA_TYPE_ROWID:
            if not self.in_fetch:
                column_value = buf.read_str(CS_FORM_IMPLICIT)
            else:
                buf.read_ub1(&num_bytes)
                if num_bytes == 0 or num_bytes == TNS_NULL_LENGTH_INDICATOR:
                    column_value = None
                else:
                    buf.read_rowid(&rowid)
                    column_value = _encode_rowid(&rowid)
        elif ora_type_num == TNS_DATA_TYPE_UROWID:
            if not self.in_fetch:
                column_value = buf.read_str(CS_FORM_IMPLICIT)
            else:
                column_value = buf.read_urowid()
        elif ora_type_num == TNS_DATA_TYPE_BINARY_DOUBLE:
            column_value = buf.read_binary_double()
        elif ora_type_num == TNS_DATA_TYPE_BINARY_FLOAT:
            column_value = buf.read_binary_float()
        elif ora_type_num == TNS_DATA_TYPE_BINARY_INTEGER:
            column_value = buf.read_oracle_number(NUM_TYPE_INT)
            if column_value is not None:
                column_value = int(column_value)
        elif ora_type_num == TNS_DATA_TYPE_CURSOR:
            buf.skip_ub1()                  # length (fixed value)
            if not self.in_fetch:
                column_value = var_impl._values[pos]
            column_value = self._create_cursor_from_describe(buf, column_value)
            cursor_impl = column_value._impl
            buf.read_ub2(&cursor_impl._statement._cursor_id)
        elif ora_type_num == TNS_DATA_TYPE_BOOLEAN:
            column_value = buf.read_bool()
        elif ora_type_num == TNS_DATA_TYPE_INTERVAL_DS:
            column_value = buf.read_interval_ds()
        elif ora_type_num == TNS_DATA_TYPE_INTERVAL_YM:
            column_value = buf.read_interval_ym()
        elif ora_type_num in (TNS_DATA_TYPE_CLOB,
                              TNS_DATA_TYPE_BLOB,
                              TNS_DATA_TYPE_BFILE):
            column_value = buf.read_lob_with_length(self.conn_impl,
                                                    var_impl.dbtype)
        elif ora_type_num == TNS_DATA_TYPE_JSON:
            column_value = buf.read_oson()
        elif ora_type_num == TNS_DATA_TYPE_VECTOR:
            column_value = buf.read_vector()
        elif ora_type_num == TNS_DATA_TYPE_INT_NAMED:
            typ_impl = var_impl.objtype
            if typ_impl is None:
                column_value = buf.read_xmltype(self.conn_impl)
            else:
                obj_impl = buf.read_dbobject(typ_impl)
                if obj_impl is not None:
                    if not self.in_fetch:
                        column_value = var_impl._values[pos]
                    if column_value is not None:
                        column_value._impl = obj_impl
                    else:
                        column_value = PY_TYPE_DB_OBJECT._from_impl(obj_impl)
        else:
            errors._raise_err(errors.ERR_DB_TYPE_NOT_SUPPORTED,
                              name=var_impl.dbtype.name)
        if not self.in_fetch:
            buf.read_sb4(&actual_num_bytes)
            if actual_num_bytes < 0 and ora_type_num == TNS_DATA_TYPE_BOOLEAN:
                column_value = None
            elif actual_num_bytes != 0 and column_value is not None:
                unit_type = "bytes" if isinstance(column_value, bytes) \
                            else "characters"
                errors._raise_err(errors.ERR_COLUMN_TRUNCATED,
                                  col_value_len=len(column_value),
                                  unit=unit_type, actual_len=actual_num_bytes)
        elif ora_type_num == TNS_DATA_TYPE_LONG \
                or ora_type_num == TNS_DATA_TYPE_LONG_RAW:
            buf.skip_sb4()                  # null indicator
            buf.skip_ub4()                  # return code
        if column_value is not None:
            if var_impl._conv_func is not None:
                column_value = var_impl._conv_func(column_value)
        return column_value

    cdef FetchInfoImpl _process_column_info(self, ReadBuffer buf,
                                            BaseThinCursorImpl cursor_impl):
        cdef:
            uint32_t num_bytes, uds_flags, num_annotations, i
            ThinDbObjectTypeImpl typ_impl
            str schema, name, key, value
            uint8_t data_type, csfrm
            FetchInfoImpl fetch_info
            int8_t precision, scale
            uint8_t nulls_allowed
            int cache_num
            bytes oid
        buf.read_ub1(&data_type)
        fetch_info = FetchInfoImpl()
        buf.skip_ub1()                      # flags
        buf.read_sb1(&precision)
        fetch_info.precision = precision
        buf.read_sb1(&scale)
        fetch_info.scale = scale
        buf.read_ub4(&fetch_info.buffer_size)
        buf.skip_ub4()                      # max number of array elements
        buf.skip_ub8()                      # cont flags
        buf.read_ub4(&num_bytes)            # OID
        if num_bytes > 0:
            oid = buf.read_bytes()
        buf.skip_ub2()                      # version
        buf.skip_ub2()                      # character set id
        buf.read_ub1(&csfrm)                # character set form
        fetch_info.dbtype = DbType._from_ora_type_and_csfrm(data_type, csfrm)
        buf.read_ub4(&fetch_info.size)
        if data_type == TNS_DATA_TYPE_RAW:
            fetch_info.size = fetch_info.buffer_size
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2:
            buf.skip_ub4()                  # oaccolid
        buf.read_ub1(&nulls_allowed)
        fetch_info.nulls_allowed = nulls_allowed
        buf.skip_ub1()                      # v7 length of name
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            fetch_info.name = buf.read_str(CS_FORM_IMPLICIT)
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            schema = buf.read_str(CS_FORM_IMPLICIT)
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            name = buf.read_str(CS_FORM_IMPLICIT)
        buf.skip_ub2()                      # column position
        buf.read_ub4(&uds_flags)
        fetch_info.is_json = uds_flags & TNS_UDS_FLAGS_IS_JSON
        fetch_info.is_oson = uds_flags & TNS_UDS_FLAGS_IS_OSON
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1:
            buf.read_ub4(&num_bytes)
            if num_bytes > 0:
                fetch_info.domain_schema = buf.read_str(CS_FORM_IMPLICIT)
            buf.read_ub4(&num_bytes)
            if num_bytes > 0:
                fetch_info.domain_name = buf.read_str(CS_FORM_IMPLICIT)
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3:
            buf.read_ub4(&num_annotations)
            if num_annotations > 0:
                buf.skip_ub1()
                fetch_info.annotations = {}
                buf.read_ub4(&num_annotations)
                buf.skip_ub1()
                for i in range(num_annotations):
                    buf.skip_ub4()          # length of key
                    key = buf.read_str(CS_FORM_IMPLICIT)
                    buf.read_ub4(&num_bytes)
                    if num_bytes > 0:
                        value = buf.read_str(CS_FORM_IMPLICIT)
                    else:
                        value = ""
                    fetch_info.annotations[key] = value
                    buf.skip_ub4()          # flags
                buf.skip_ub4()              # flags
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4:
            buf.read_ub4(&fetch_info.vector_dimensions)
            buf.read_ub1(&fetch_info.vector_format)
            buf.read_ub1(&fetch_info.vector_flags)
        if data_type == TNS_DATA_TYPE_INT_NAMED:
            if self.type_cache is None:
                cache_num = self.conn_impl._dbobject_type_cache_num
                self.type_cache = get_dbobject_type_cache(cache_num)
            typ_impl = self.type_cache.get_type_for_info(oid, schema, None,
                                                         name)
            if typ_impl.is_xml_type:
                fetch_info.dbtype = DB_TYPE_XMLTYPE
            else:
                fetch_info.objtype = typ_impl
        return fetch_info

    cdef int _process_describe_info(self, ReadBuffer buf,
                                    object cursor,
                                    BaseThinCursorImpl cursor_impl) except -1:
        cdef:
            Statement stmt = cursor_impl._statement
            list prev_fetch_var_impls
            object type_handler, conn
            FetchInfoImpl fetch_info
            uint32_t num_bytes, i
            bint uses_fetch_info
            str message
        buf.skip_ub4()                      # max row size
        buf.read_ub4(&cursor_impl._num_columns)
        prev_fetch_var_impls = stmt._fetch_var_impls
        cursor_impl._init_fetch_vars(cursor_impl._num_columns)
        if cursor_impl._num_columns > 0:
            buf.skip_ub1()
        type_handler = cursor_impl._get_output_type_handler(&uses_fetch_info)
        conn = self.cursor.connection
        for i in range(cursor_impl._num_columns):
            fetch_info = self._process_column_info(buf, cursor_impl)
            if prev_fetch_var_impls is not None \
                    and i < len(prev_fetch_var_impls):
                self._adjust_fetch_info(prev_fetch_var_impls[i], fetch_info)
            if fetch_info.dbtype._ora_type_num in (TNS_DATA_TYPE_BLOB,
                                                   TNS_DATA_TYPE_CLOB,
                                                   TNS_DATA_TYPE_JSON,
                                                   TNS_DATA_TYPE_VECTOR):
                stmt._requires_define = True
                stmt._no_prefetch = True
            cursor_impl._create_fetch_var(conn, self.cursor, type_handler,
                                          uses_fetch_info, i, fetch_info)
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            buf.skip_raw_bytes_chunked()    # current date
        buf.skip_ub4()                      # dcbflag
        buf.skip_ub4()                      # dcbmdbz
        buf.skip_ub4()                      # dcbmnpr
        buf.skip_ub4()                      # dcbmxpr
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            buf.skip_raw_bytes_chunked()    # dcbqcky
        stmt._fetch_info_impls = cursor_impl.fetch_info_impls
        stmt._fetch_vars = cursor_impl.fetch_vars
        stmt._fetch_var_impls = cursor_impl.fetch_var_impls
        stmt._num_columns = cursor_impl._num_columns
        stmt._last_output_type_handler = type_handler

    cdef int _process_error_info(self, ReadBuffer buf) except -1:
        cdef:
            BaseThinCursorImpl cursor_impl = self.cursor_impl
            BaseThinConnImpl conn_impl = self.conn_impl
            object exc_type
        Message._process_error_info(self, buf)
        if self.error_info.cursor_id != 0:
            cursor_impl._statement._cursor_id = self.error_info.cursor_id
        if not cursor_impl._statement._is_plsql and not self.in_fetch:
            cursor_impl.rowcount = self.error_info.rowcount
        elif self.in_fetch and self.row_index > 0:
            cursor_impl._statement._requires_define = False
        cursor_impl._lastrowid = self.error_info.rowid
        cursor_impl._batcherrors = self.error_info.batcherrors
        if self.batcherrors and cursor_impl._batcherrors is None:
            cursor_impl._batcherrors = []
        if self.error_info.num == TNS_ERR_NO_DATA_FOUND and self.in_fetch:
            self.error_info.num = 0
            cursor_impl._more_rows_to_fetch = False
            cursor_impl._last_row_index = 0
            cursor_impl._statement._requires_define = False
            self.error_occurred = False
        elif self.error_info.num == TNS_ERR_ARRAY_DML_ERRORS:
            self.error_info.num = 0
            self.error_occurred = False
        elif self.retry:
            self.retry = False
        elif cursor_impl._statement._is_query \
                and self.error_info.num in (TNS_ERR_VAR_NOT_IN_SELECT_LIST,
                                            TNS_ERR_INCONSISTENT_DATA_TYPES):
            self.retry = True
            conn_impl._statement_cache.clear_cursor(cursor_impl._statement)
        elif self.error_info.num != 0 and self.error_info.cursor_id != 0:
            if self.error_info.num not in errors.ERR_INTEGRITY_ERROR_CODES:
                conn_impl._statement_cache.clear_cursor(cursor_impl._statement)

    cdef int _process_implicit_result(self, ReadBuffer buf) except -1:
        cdef:
            BaseThinCursorImpl child_cursor_impl
            uint32_t i, num_results
            object child_cursor
            uint8_t num_bytes
        self.cursor_impl._implicit_resultsets = []
        buf.read_ub4(&num_results)
        for i in range(num_results):
            buf.read_ub1(&num_bytes)
            buf.skip_raw_bytes(num_bytes)
            child_cursor = self._create_cursor_from_describe(buf)
            child_cursor_impl = child_cursor._impl
            buf.read_ub2(&child_cursor_impl._statement._cursor_id)
            self.cursor_impl._implicit_resultsets.append(child_cursor)

    cdef int _process_io_vector(self, ReadBuffer buf) except -1:
        """
        An I/O vector is sent by the database in response to a PL/SQL execute.
        It indicates whether binds are IN only, IN/OUT or OUT only.
        """
        cdef:
            uint16_t i, num_bytes, temp16
            uint32_t temp32, num_binds
            BindInfo bind_info
        buf.skip_ub1()                      # flag
        buf.read_ub2(&temp16)               # num requests
        buf.read_ub4(&temp32)               # num iters
        num_binds = temp32 * 256 + temp16
        buf.skip_ub4()                      # num iters this time
        buf.skip_ub2()                      # uac buffer length
        buf.read_ub2(&num_bytes)            # bit vector for fast fetch
        if num_bytes > 0:
            buf.skip_raw_bytes(num_bytes)
        buf.read_ub2(&num_bytes)            # rowid
        if num_bytes > 0:
            buf.skip_raw_bytes(num_bytes)
        self.out_var_impls = []
        for i in range(num_binds):          # bind directions
            bind_info = self.cursor_impl._statement._bind_info_list[i]
            buf.read_ub1(&bind_info.bind_dir)
            if bind_info.bind_dir == TNS_BIND_DIR_INPUT:
                continue
            self.out_var_impls.append(bind_info._bind_var_impl)

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        if message_type == TNS_MSG_TYPE_ROW_HEADER:
            self._process_row_header(buf)
        elif message_type == TNS_MSG_TYPE_ROW_DATA:
            self._process_row_data(buf)
        elif message_type == TNS_MSG_TYPE_FLUSH_OUT_BINDS:
            self.flush_out_binds = True
            self.end_of_response = True
        elif message_type == TNS_MSG_TYPE_DESCRIBE_INFO:
            buf.skip_raw_bytes_chunked()
            self._process_describe_info(buf, self.cursor, self.cursor_impl)
            self.out_var_impls = self.cursor_impl.fetch_var_impls
        elif message_type == TNS_MSG_TYPE_ERROR:
            self._process_error_info(buf)
        elif message_type == TNS_MSG_TYPE_BIT_VECTOR:
            self._process_bit_vector(buf)
        elif message_type == TNS_MSG_TYPE_IO_VECTOR:
            self._process_io_vector(buf)
        elif message_type == TNS_MSG_TYPE_IMPLICIT_RESULTSET:
            self._process_implicit_result(buf)
        else:
            Message._process_message(self, buf, message_type)

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        cdef:
            uint16_t keyword_num, num_params, num_bytes
            uint32_t num_rows, i
            uint64_t rowcount
            bytes key_value
            list rowcounts
        buf.read_ub2(&num_params)           # al8o4l (ignored)
        for i in range(num_params):
            buf.skip_ub4()
        buf.read_ub2(&num_bytes)            # al8txl (ignored)
        if num_bytes > 0:
            buf.skip_raw_bytes(num_bytes)
        buf.read_ub2(&num_params)           # num key/value pairs
        for i in range(num_params):
            buf.read_ub2(&num_bytes)        # key
            if num_bytes > 0:
                key_value = buf.read_bytes()
            buf.read_ub2(&num_bytes)        # value
            if num_bytes > 0:
                buf.skip_raw_bytes_chunked()
            buf.read_ub2(&keyword_num)      # keyword num
            if keyword_num == TNS_KEYWORD_NUM_CURRENT_SCHEMA:
                self.conn_impl._current_schema = key_value.decode()
            elif keyword_num == TNS_KEYWORD_NUM_EDITION:
                self.conn_impl._edition = key_value.decode()
        buf.read_ub2(&num_bytes)            # registration
        if num_bytes > 0:
            buf.skip_raw_bytes(num_bytes)
        if self.arraydmlrowcounts:
            buf.read_ub4(&num_rows)
            rowcounts = self.cursor_impl._dmlrowcounts = []
            for i in range(num_rows):
                buf.read_ub8(&rowcount)
                rowcounts.append(rowcount)

    cdef int _process_row_data(self, ReadBuffer buf) except -1:
        cdef:
            uint32_t num_rows, pos
            ThinVarImpl var_impl
            ssize_t i, j
            object value
            list values
        for i, var_impl in enumerate(self.out_var_impls):
            if var_impl.is_array:
                buf.read_ub4(&var_impl.num_elements_in_array)
                for pos in range(var_impl.num_elements_in_array):
                    value = self._process_column_data(buf, var_impl, pos)
                    var_impl._values[pos] = value
            elif self.cursor_impl._statement._is_returning:
                buf.read_ub4(&num_rows)
                values = [None] * num_rows
                for j in range(num_rows):
                    values[j] = self._process_column_data(buf, var_impl, j)
                var_impl._values[self.row_index] = values
            elif self._is_duplicate_data(i):
                if self.row_index == 0 and var_impl.outconverter is not None:
                    value = var_impl._last_raw_value
                else:
                    value = var_impl._values[self.cursor_impl._last_row_index]
                var_impl._values[self.row_index] = value
            else:
                value = self._process_column_data(buf, var_impl,
                                                  self.row_index)
                var_impl._values[self.row_index] = value
        self.row_index += 1
        if self.in_fetch:
            self.cursor_impl._last_row_index = self.row_index - 1
            self.cursor_impl._buffer_rowcount = self.row_index
            self.bit_vector = NULL

    cdef int _process_row_header(self, ReadBuffer buf) except -1:
        cdef uint32_t num_bytes
        buf.skip_ub1()                      # flags
        buf.skip_ub2()                      # num requests
        buf.skip_ub4()                      # iteration number
        buf.skip_ub4()                      # num iters
        buf.skip_ub2()                      # buffer length
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            buf.skip_ub1()                  # skip repeated length
            self._get_bit_vector(buf, num_bytes)
        buf.read_ub4(&num_bytes)
        if num_bytes > 0:
            buf.skip_raw_bytes_chunked()    # rxhrid

    cdef int _write_column_metadata(self, WriteBuffer buf,
                                    list bind_var_impls) except -1:
        cdef:
            uint32_t buffer_size, cont_flag, lob_prefetch_length
            ThinDbObjectTypeImpl typ_impl
            uint8_t ora_type_num, flag
            ThinVarImpl var_impl
        for var_impl in bind_var_impls:
            ora_type_num = var_impl.dbtype._ora_type_num
            buffer_size = var_impl.buffer_size
            if ora_type_num in (TNS_DATA_TYPE_ROWID, TNS_DATA_TYPE_UROWID):
                ora_type_num = TNS_DATA_TYPE_VARCHAR
                buffer_size = TNS_MAX_UROWID_LENGTH
            flag = TNS_BIND_USE_INDICATORS
            if var_impl.is_array:
                flag |= TNS_BIND_ARRAY
            cont_flag = 0
            lob_prefetch_length = 0
            if ora_type_num in (TNS_DATA_TYPE_BLOB,
                                TNS_DATA_TYPE_CLOB):
                cont_flag = TNS_LOB_PREFETCH_FLAG
            elif ora_type_num == TNS_DATA_TYPE_JSON:
                cont_flag = TNS_LOB_PREFETCH_FLAG
                buffer_size = lob_prefetch_length = TNS_JSON_MAX_LENGTH
            elif ora_type_num == TNS_DATA_TYPE_VECTOR:
                cont_flag = TNS_LOB_PREFETCH_FLAG
                buffer_size = lob_prefetch_length = TNS_VECTOR_MAX_LENGTH
            buf.write_uint8(ora_type_num)
            buf.write_uint8(flag)
            # precision and scale are always written as zero as the server
            # expects that and complains if any other value is sent!
            buf.write_uint8(0)
            buf.write_uint8(0)
            if buffer_size > buf._caps.max_string_size:
                buf.write_ub4(TNS_MAX_LONG_LENGTH)
            else:
                buf.write_ub4(buffer_size)
            if var_impl.is_array:
                buf.write_ub4(var_impl.num_elements)
            else:
                buf.write_ub4(0)            # max num elements
            buf.write_ub8(cont_flag)
            if var_impl.objtype is not None:
                typ_impl = var_impl.objtype
                buf.write_ub4(len(typ_impl.oid))
                buf.write_bytes_with_length(typ_impl.oid)
                buf.write_ub4(typ_impl.version)
            else:
                buf.write_ub4(0)            # OID
                buf.write_ub2(0)            # version
            if var_impl.dbtype._csfrm != 0:
                buf.write_ub2(TNS_CHARSET_UTF8)
            else:
                buf.write_ub2(0)
            buf.write_uint8(var_impl.dbtype._csfrm)
            buf.write_ub4(lob_prefetch_length)  # max chars (LOB prefetch)
            if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2:
                buf.write_ub4(0)            # oaccolid

    cdef int _write_begin_pipeline_piggyback(self, WriteBuffer buf) except -1:
        buf._data_flags |= TNS_DATA_FLAGS_BEGIN_PIPELINE
        self._write_piggyback_code(buf, TNS_FUNC_PIPELINE_BEGIN)
        buf.write_ub2(0)                    # error set ID
        buf.write_uint8(0)                  # error set mode
        buf.write_uint8(self.conn_impl.pipeline_mode)

    cdef int _write_bind_params_column(self, WriteBuffer buf,
                                       ThinVarImpl var_impl,
                                       object value) except -1:
        cdef:
            uint8_t ora_type_num = var_impl.dbtype._ora_type_num
            ThinDbObjectTypeImpl typ_impl
            BaseThinCursorImpl cursor_impl
            BaseThinLobImpl lob_impl
            uint32_t num_bytes
            bytes temp_bytes
        if value is None:
            if ora_type_num == TNS_DATA_TYPE_BOOLEAN:
                buf.write_uint8(TNS_ESCAPE_CHAR)
                buf.write_uint8(1)
            elif ora_type_num == TNS_DATA_TYPE_INT_NAMED:
                buf.write_ub4(0)                # TOID
                buf.write_ub4(0)                # OID
                buf.write_ub4(0)                # snapshot
                buf.write_ub2(0)                # version
                buf.write_ub4(0)                # packed data length
                buf.write_ub4(TNS_OBJ_TOP_LEVEL)    # flags
            else:
                buf.write_uint8(0)
        elif ora_type_num == TNS_DATA_TYPE_VARCHAR \
                or ora_type_num == TNS_DATA_TYPE_CHAR \
                or ora_type_num == TNS_DATA_TYPE_LONG:
            if var_impl.dbtype._csfrm == CS_FORM_IMPLICIT:
                temp_bytes = (<str> value).encode()
            else:
                buf._caps._check_ncharset_id()
                temp_bytes = (<str> value).encode(ENCODING_UTF16)
            buf.write_bytes_with_length(temp_bytes)
        elif ora_type_num == TNS_DATA_TYPE_RAW \
                or ora_type_num == TNS_DATA_TYPE_LONG_RAW:
            buf.write_bytes_with_length(value)
        elif ora_type_num == TNS_DATA_TYPE_NUMBER \
                or ora_type_num == TNS_DATA_TYPE_BINARY_INTEGER:
            if isinstance(value, bool):
                temp_bytes = b'1' if value is True else b'0'
            else:
                temp_bytes = (<str> cpython.PyObject_Str(value)).encode()
            buf.write_oracle_number(temp_bytes)
        elif ora_type_num == TNS_DATA_TYPE_DATE \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP_TZ \
                or ora_type_num == TNS_DATA_TYPE_TIMESTAMP_LTZ:
            buf.write_oracle_date(value, var_impl.dbtype._buffer_size_factor)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_DOUBLE:
            buf.write_binary_double(value)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_FLOAT:
            buf.write_binary_float(value)
        elif ora_type_num == TNS_DATA_TYPE_CURSOR:
            cursor_impl = value._impl
            if cursor_impl is None:
                errors._raise_err(errors.ERR_CURSOR_NOT_OPEN)
            if cursor_impl._statement is None:
                cursor_impl._statement = self.conn_impl._get_statement()
            if cursor_impl._statement._cursor_id == 0:
                buf.write_uint8(1)
                buf.write_uint8(0)
            else:
                buf.write_ub4(1)
                buf.write_ub4(cursor_impl._statement._cursor_id)
            cursor_impl.statement = None
        elif ora_type_num == TNS_DATA_TYPE_BOOLEAN:
            buf.write_bool(value)
        elif ora_type_num == TNS_DATA_TYPE_INTERVAL_DS:
            buf.write_interval_ds(value)
        elif ora_type_num == TNS_DATA_TYPE_INTERVAL_YM:
            buf.write_interval_ym(value)
        elif ora_type_num in (
                TNS_DATA_TYPE_BLOB,
                TNS_DATA_TYPE_CLOB,
                TNS_DATA_TYPE_BFILE
            ):
            buf.write_lob_with_length(value._impl)
        elif ora_type_num in (TNS_DATA_TYPE_ROWID, TNS_DATA_TYPE_UROWID):
            temp_bytes = (<str> value).encode()
            buf.write_bytes_with_length(temp_bytes)
        elif ora_type_num == TNS_DATA_TYPE_INT_NAMED:
            buf.write_dbobject(value._impl)
        elif ora_type_num == TNS_DATA_TYPE_JSON:
            buf.write_oson(value, self.conn_impl._oson_max_fname_size)
        elif ora_type_num == TNS_DATA_TYPE_VECTOR:
            buf.write_vector(value)
        else:
            errors._raise_err(errors.ERR_DB_TYPE_NOT_SUPPORTED,
                              name=var_impl.dbtype.name)

    cdef int _write_bind_params_row(self, WriteBuffer buf, list params,
                                    uint32_t pos) except -1:
        """
        Write a row of bind parameters. Note that non-LONG values are written
        first followed by any LONG values.
        """
        cdef:
            uint32_t i, num_elements, offset = self.offset
            bint found_long = False
            ThinVarImpl var_impl
            BindInfo bind_info
        for i, bind_info in enumerate(params):
            if bind_info._is_return_bind:
                continue
            var_impl = bind_info._bind_var_impl
            if var_impl.is_array:
                num_elements = var_impl.num_elements_in_array
                buf.write_ub4(num_elements)
                for value in var_impl._values[:num_elements]:
                    self._write_bind_params_column(buf, var_impl, value)
            else:
                if not self.cursor_impl._statement._is_plsql \
                        and var_impl.buffer_size > buf._caps.max_string_size:
                    found_long = True
                    continue
                self._write_bind_params_column(buf, var_impl,
                                               var_impl._values[pos + offset])
        if found_long:
            for i, bind_info in enumerate(params):
                if bind_info._is_return_bind:
                    continue
                var_impl = bind_info._bind_var_impl
                if var_impl.buffer_size <= buf._caps.max_string_size:
                    continue
                self._write_bind_params_column(buf, var_impl,
                                               var_impl._values[pos + offset])

    cdef int _write_close_cursors_piggyback(self, WriteBuffer buf) except -1:
        self._write_piggyback_code(buf, TNS_FUNC_CLOSE_CURSORS)
        buf.write_uint8(1)                  # pointer
        self.conn_impl._statement_cache.write_cursors_to_close(buf)

    cdef int _write_current_schema_piggyback(self, WriteBuffer buf) except -1:
        cdef bytes schema_bytes
        self._write_piggyback_code(buf, TNS_FUNC_SET_SCHEMA)
        buf.write_uint8(1)                  # pointer
        schema_bytes = self.conn_impl._current_schema.encode()
        buf.write_ub4(len(schema_bytes))
        buf.write_bytes_with_length(schema_bytes)

    cdef int _write_close_temp_lobs_piggyback(self,
                                              WriteBuffer buf) except -1:
        cdef:
            list lobs_to_close = self.conn_impl._temp_lobs_to_close
            uint64_t total_size = 0
        self._write_piggyback_code(buf, TNS_FUNC_LOB_OP)
        op_code = TNS_LOB_OP_FREE_TEMP | TNS_LOB_OP_ARRAY

        # temp lob data
        buf.write_uint8(1)                  # pointer
        buf.write_ub4(self.conn_impl._temp_lobs_total_size)
        buf.write_uint8(0)                  # dest lob locator
        buf.write_ub4(0)
        buf.write_ub4(0)                    # source lob locator
        buf.write_ub4(0)
        buf.write_uint8(0)                  # source lob offset
        buf.write_uint8(0)                  # dest lob offset
        buf.write_uint8(0)                  # charset
        buf.write_ub4(op_code)
        buf.write_uint8(0)                  # scn
        buf.write_ub4(0)                    # losbscn
        buf.write_ub8(0)                    # lobscnl
        buf.write_ub8(0)
        buf.write_uint8(0)

        # array lob fields
        buf.write_uint8(0)
        buf.write_ub4(0)
        buf.write_uint8(0)
        buf.write_ub4(0)
        buf.write_uint8(0)
        buf.write_ub4(0)
        for i in range(len(lobs_to_close)):
            buf.write_bytes(lobs_to_close[i])

        # reset values
        self.conn_impl._temp_lobs_to_close = None
        self.conn_impl._temp_lobs_total_size = 0

    cdef int _write_end_to_end_piggyback(self, WriteBuffer buf) except -1:
        cdef:
            bytes action_bytes, client_identifier_bytes, client_info_bytes
            BaseThinConnImpl conn_impl = self.conn_impl
            bytes module_bytes, dbop_bytes
            uint32_t flags = 0

        # determine which flags to send
        if conn_impl._action_modified:
            flags |= TNS_END_TO_END_ACTION
        if conn_impl._client_identifier_modified:
            flags |= TNS_END_TO_END_CLIENT_IDENTIFIER
        if conn_impl._client_info_modified:
            flags |= TNS_END_TO_END_CLIENT_INFO
        if conn_impl._module_modified:
            flags |= TNS_END_TO_END_MODULE
        if conn_impl._dbop_modified:
            flags |= TNS_END_TO_END_DBOP

        # write initial packet data
        self._write_piggyback_code(buf, TNS_FUNC_SET_END_TO_END_ATTR)
        buf.write_uint8(0)                  # pointer (cidnam)
        buf.write_uint8(0)                  # pointer (cidser)
        buf.write_ub4(flags)

        # write client identifier header info
        if conn_impl._client_identifier_modified:
            buf.write_uint8(1)              # pointer (client identifier)
            if conn_impl._client_identifier is None:
                buf.write_ub4(0)
            else:
                client_identifier_bytes = conn_impl._client_identifier.encode()
                buf.write_ub4(len(client_identifier_bytes))
        else:
            buf.write_uint8(0)              # pointer (client identifier)
            buf.write_ub4(0)                # length of client identifier

        # write module header info
        if conn_impl._module_modified:
            buf.write_uint8(1)              # pointer (module)
            if conn_impl._module is None:
                buf.write_ub4(0)
            else:
                module_bytes = conn_impl._module.encode()
                buf.write_ub4(len(module_bytes))
        else:
            buf.write_uint8(0)              # pointer (module)
            buf.write_ub4(0)                # length of module

        # write action header info
        if conn_impl._action_modified:
            buf.write_uint8(1)              # pointer (action)
            if conn_impl._action is None:
                buf.write_ub4(0)
            else:
                action_bytes = conn_impl._action.encode()
                buf.write_ub4(len(action_bytes))
        else:
            buf.write_uint8(0)              # pointer (action)
            buf.write_ub4(0)                # length of action

        # write unsupported bits
        buf.write_uint8(0)                  # pointer (cideci)
        buf.write_ub4(0)                    # length (cideci)
        buf.write_uint8(0)                  # cidcct
        buf.write_ub4(0)                    # cidecs

        # write client info header info
        if conn_impl._client_info_modified:
            buf.write_uint8(1)              # pointer (client info)
            if conn_impl._client_info is None:
                buf.write_ub4(0)
            else:
                client_info_bytes = conn_impl._client_info.encode()
                buf.write_ub4(len(client_info_bytes))
        else:
            buf.write_uint8(0)              # pointer (client info)
            buf.write_ub4(0)                # length of client info

        # write more unsupported bits
        buf.write_uint8(0)                  # pointer (cidkstk)
        buf.write_ub4(0)                    # length (cidkstk)
        buf.write_uint8(0)                  # pointer (cidktgt)
        buf.write_ub4(0)                    # length (cidktgt)

        # write dbop header info
        if conn_impl._dbop_modified:
            buf.write_uint8(1)              # pointer (dbop)
            if conn_impl._dbop is None:
                buf.write_ub4(0)
            else:
                dbop_bytes = conn_impl._dbop.encode()
                buf.write_ub4(len(dbop_bytes))
        else:
            buf.write_uint8(0)              # pointer (dbop)
            buf.write_ub4(0)                # length of dbop

        # write strings
        if conn_impl._client_identifier_modified \
                and conn_impl._client_identifier is not None:
            buf.write_bytes_with_length(client_identifier_bytes)
        if conn_impl._module_modified and conn_impl._module is not None:
            buf.write_bytes_with_length(module_bytes)
        if conn_impl._action_modified and conn_impl._action is not None:
            buf.write_bytes_with_length(action_bytes)
        if conn_impl._client_info_modified \
                and conn_impl._client_info is not None:
            buf.write_bytes_with_length(client_info_bytes)
        if conn_impl._dbop_modified and conn_impl._dbop is not None:
            buf.write_bytes_with_length(dbop_bytes)

        # reset flags and values
        conn_impl._action_modified = False
        conn_impl._action = None
        conn_impl._client_identifier_modified = False
        conn_impl._client_identifier = None
        conn_impl._client_info_modified = False
        conn_impl._client_info = None
        conn_impl._dbop_modified = False
        conn_impl._dbop = None
        conn_impl._module_modified = False
        conn_impl._module = None

    cdef int _write_piggybacks(self, WriteBuffer buf) except -1:
        if self.conn_impl.pipeline_mode != 0:
            self._write_begin_pipeline_piggyback(buf)
            self.conn_impl.pipeline_mode = 0
        if self.conn_impl._current_schema_modified:
            self._write_current_schema_piggyback(buf)
        if self.conn_impl._statement_cache._num_cursors_to_close > 0 \
                and not self.conn_impl._drcp_establish_session:
            self._write_close_cursors_piggyback(buf)
        if self.conn_impl._action_modified \
                or self.conn_impl._client_identifier_modified \
                or self.conn_impl._client_info_modified \
                or self.conn_impl._dbop_modified \
                or self.conn_impl._module_modified:
            self._write_end_to_end_piggyback(buf)
        if self.conn_impl._temp_lobs_total_size > 0:
            self._write_close_temp_lobs_piggyback(buf)

    cdef int postprocess(self) except -1:
        """
        Run any variable out converter functions on all non-null values that
        were returned in the current database response. This must be done
        independently since the out converter function may itself invoke a
        database round-trip.
        """
        cdef:
            uint32_t i, j, num_elements
            object value, element_value
            ThinVarImpl var_impl
        if self.out_var_impls is None:
            return 0
        for var_impl in self.out_var_impls:
            if var_impl is None or var_impl.outconverter is None:
                continue
            var_impl._last_raw_value = \
                    var_impl._values[self.cursor_impl._last_row_index]
            if var_impl.is_array:
                num_elements = var_impl.num_elements_in_array
            else:
                num_elements = self.row_index
            for i in range(num_elements):
                value = var_impl._values[i]
                if value is None and not var_impl.convert_nulls:
                    continue
                if isinstance(value, list):
                    for j, element_value in enumerate(value):
                        if element_value is None:
                            continue
                        value[j] = var_impl.outconverter(element_value)
                else:
                    var_impl._values[i] = var_impl.outconverter(value)

    async def postprocess_async(self):
        """
        Run any variable out converter functions on all non-null values that
        were returned in the current database response. This must be done
        independently since the out converter function may itself invoke a
        database round-trip.
        """
        cdef:
            object value, element_value, fn
            uint32_t i, j, num_elements
            ThinVarImpl var_impl
        if self.out_var_impls is None:
            return 0
        for var_impl in self.out_var_impls:
            if var_impl is None or var_impl.outconverter is None:
                continue
            var_impl._last_raw_value = \
                    var_impl._values[self.cursor_impl._last_row_index]
            if var_impl.is_array:
                num_elements = var_impl.num_elements_in_array
            else:
                num_elements = self.row_index
            fn = var_impl.outconverter
            for i in range(num_elements):
                value = var_impl._values[i]
                if value is None and not var_impl.convert_nulls:
                    continue
                if isinstance(value, list):
                    for j, element_value in enumerate(value):
                        if element_value is None:
                            continue
                        element_value = fn(element_value)
                        if inspect.isawaitable(element_value):
                            element_value = await element_value
                        value[j] = element_value
                else:
                    value = fn(value)
                    if inspect.isawaitable(value):
                        value = await value
                    var_impl._values[i] = value

    cdef int preprocess(self) except -1:
        cdef:
            Statement statement = self.cursor_impl._statement
            BindInfo bind_info
        if statement._is_returning and not self.parse_only:
            self.out_var_impls = []
            for bind_info in statement._bind_info_list:
                if not bind_info._is_return_bind:
                    continue
                self.out_var_impls.append(bind_info._bind_var_impl)
        elif statement._is_query:
            self._preprocess_query()


cdef class AuthMessage(Message):
    cdef:
        str encoded_password
        bytes password
        bytes newpassword
        str encoded_newpassword
        str encoded_jdwp_data
        str debug_jdwp
        str session_key
        str speedy_key
        str proxy_user
        str token
        str private_key
        str service_name
        uint8_t purity
        ssize_t user_bytes_len
        bytes user_bytes
        dict session_data
        uint32_t auth_mode
        uint32_t verifier_type
        bint change_password
        str program
        str terminal
        str machine
        str osuser
        str driver_name
        str edition
        list appcontext

    cdef int _encrypt_passwords(self) except -1:
        """
        Encrypts the passwords using the session key.
        """

        # encrypt password
        salt = secrets.token_bytes(16)
        password_with_salt = salt + self.password
        encrypted_password = encrypt_cbc(self.conn_impl._combo_key,
                                         password_with_salt)
        self.encoded_password = encrypted_password.hex().upper()

        # encrypt new password
        if self.newpassword is not None:
            newpassword_with_salt = salt + self.newpassword
            encrypted_newpassword = encrypt_cbc(self.conn_impl._combo_key,
                                                newpassword_with_salt)
            self.encoded_newpassword = encrypted_newpassword.hex().upper()

    cdef int _generate_verifier(self) except -1:
        """
        Generate the multi-round verifier.
        """
        cdef:
            bytes jdwp_data
            bytearray b
            ssize_t i

        # create password hash
        verifier_data = bytes.fromhex(self.session_data['AUTH_VFR_DATA'])
        if self.verifier_type == TNS_VERIFIER_TYPE_12C:
            keylen = 32
            iterations = int(self.session_data['AUTH_PBKDF2_VGEN_COUNT'])
            salt = verifier_data + b'AUTH_PBKDF2_SPEEDY_KEY'
            password_key = get_derived_key(self.password, salt, 64,
                                           iterations)
            h = hashlib.new("sha512")
            h.update(password_key)
            h.update(verifier_data)
            password_hash = h.digest()[:32]
        else:
            keylen = 24
            h = hashlib.sha1(self.password)
            h.update(verifier_data)
            password_hash = h.digest() + bytes(4)

        # decrypt first half of session key
        encoded_server_key = bytes.fromhex(self.session_data['AUTH_SESSKEY'])
        session_key_part_a = decrypt_cbc(password_hash, encoded_server_key)

        # generate second half of session key
        session_key_part_b = secrets.token_bytes(len(session_key_part_a))
        encoded_client_key = encrypt_cbc(password_hash, session_key_part_b)

        # create session key and combo key
        if len(session_key_part_a) == 48:
            self.session_key = encoded_client_key.hex().upper()[:96]
            b = bytearray(24)
            for i in range(16, 40):
                b[i - 16] = session_key_part_a[i] ^ session_key_part_b[i]
            part1 = hashlib.md5(b[:16]).digest()
            part2 = hashlib.md5(b[16:]).digest()
            combo_key = (part1 + part2)[:keylen]
        else:
            self.session_key = encoded_client_key.hex().upper()[:64]
            salt = bytes.fromhex(self.session_data['AUTH_PBKDF2_CSK_SALT'])
            iterations = int(self.session_data['AUTH_PBKDF2_SDER_COUNT'])
            temp_key = session_key_part_b[:keylen] + session_key_part_a[:keylen]
            combo_key = get_derived_key(temp_key.hex().upper().encode(), salt,
                                        keylen, iterations)

        # retain session key for use by the change password API
        self.conn_impl._combo_key = combo_key

        # generate speedy key for 12c verifiers
        if self.verifier_type == TNS_VERIFIER_TYPE_12C:
            salt = secrets.token_bytes(16)
            speedy_key = encrypt_cbc(combo_key, salt + password_key)
            self.speedy_key = speedy_key[:80].hex().upper()

        # encrypts the passwords
        self._encrypt_passwords()

        # check if debug_jdwp is set. if set, encode the data using the
        # combo session key with zeros padding
        if self.debug_jdwp is not None:
            jdwp_data = self.debug_jdwp.encode()
            encrypted_jdwp_data = encrypt_cbc(combo_key, jdwp_data, zeros=True)
            # Add a "01" at the end of the hex encrypted data to indicate the
            # use of AES encryption
            self.encoded_jdwp_data = encrypted_jdwp_data.hex().upper() + "01"

    cdef str _get_alter_timezone_statement(self):
        """
        Returns the statement required to change the session time zone to match
        the time zone in use by the Python interpreter.
        """
        cdef:
            int tz_hour, tz_minute, timezone
            str sign, tz_repr
        tz_repr = os.environ.get("ORA_SDTZ")
        if tz_repr is None:
            timezone = time.localtime().tm_gmtoff
            tz_hour = timezone // 3600
            tz_minute = (timezone - (tz_hour * 3600)) // 60
            if tz_hour < 0:
                sign = "-"
                tz_hour = -tz_hour
            else:
                sign = "+"
            tz_repr = f"{sign}{tz_hour:02}:{tz_minute:02}"
        return f"ALTER SESSION SET TIME_ZONE='{tz_repr}'\x00"

    cdef tuple _get_version_tuple(self, ReadBuffer buf):
        """
        Return the 5-tuple for the database version. Note that the format
        changed with Oracle Database 18.
        """
        cdef uint32_t full_version_num
        full_version_num = int(self.session_data["AUTH_VERSION_NO"])
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_18_1_EXT_1:
            return ((full_version_num >> 24) & 0xFF,
                    (full_version_num >> 16) & 0xFF,
                    (full_version_num >> 12) & 0x0F,
                    (full_version_num >> 4) & 0xFF,
                    (full_version_num & 0x0F))
        else:
            return ((full_version_num >> 24) & 0xFF,
                    (full_version_num >> 20) & 0x0F,
                    (full_version_num >> 12) & 0x0F,
                    (full_version_num >> 8) & 0x0F,
                    (full_version_num & 0x0F))

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_AUTH_PHASE_ONE
        self.session_data = {}
        if self.conn_impl.username is not None:
            self.user_bytes = self.conn_impl.username.encode()
            self.user_bytes_len = len(self.user_bytes)
        self.resend = True

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        cdef:
            uint16_t num_params, i
            uint32_t num_bytes
            str key, value
        buf.read_ub2(&num_params)
        for i in range(num_params):
            buf.skip_ub4()
            key = buf.read_str(CS_FORM_IMPLICIT)
            buf.read_ub4(&num_bytes)
            if num_bytes > 0:
                value = buf.read_str(CS_FORM_IMPLICIT)
            else:
                value = ""
            if key == "AUTH_VFR_DATA":
                buf.read_ub4(&self.verifier_type)
            else:
                buf.skip_ub4()                  # skip flags
            self.session_data[key] = value
        if self.function_code == TNS_FUNC_AUTH_PHASE_ONE:
            self.function_code = TNS_FUNC_AUTH_PHASE_TWO
        elif not self.change_password:
            self.conn_impl._session_id = \
                    <uint32_t> int(self.session_data["AUTH_SESSION_ID"])
            self.conn_impl._serial_num = \
                    <uint16_t> int(self.session_data["AUTH_SERIAL_NUM"])
            self.conn_impl._db_domain = \
                    self.session_data.get("AUTH_SC_DB_DOMAIN")
            self.conn_impl._db_name = \
                    self.session_data.get("AUTH_SC_DBUNIQUE_NAME")
            self.conn_impl._max_open_cursors = \
                    int(self.session_data.get("AUTH_MAX_OPEN_CURSORS", 0))
            self.conn_impl._service_name = \
                    self.session_data.get("AUTH_SC_SERVICE_NAME")
            self.conn_impl._instance_name = \
                    self.session_data.get("AUTH_INSTANCENAME")
            self.conn_impl._max_identifier_length = \
                    int(self.session_data.get("AUTH_MAX_IDEN_LENGTH", 30))
            self.conn_impl.server_version = self._get_version_tuple(buf)
            self.conn_impl.supports_bool = \
                    buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1
            self.conn_impl._edition = self.edition

    cdef int _set_params(self, ConnectParamsImpl params,
                         Description description) except -1:
        """
        Sets the parameters to use for the AuthMessage. The user and auth mode
        are retained in order to avoid duplicating this effort for both trips
        to the server.
        """
        self.password = params._get_password()
        self.newpassword = params._get_new_password()
        self.service_name = description.service_name
        self.proxy_user = params.proxy_user
        self.debug_jdwp = params.debug_jdwp
        self.program = params.program
        self.terminal = params.terminal
        self.machine = params.machine
        self.osuser = params.osuser
        self.driver_name = params.driver_name
        if self.driver_name is None:
            self.driver_name = f"{DRIVER_NAME} thn : {DRIVER_VERSION}"
        self.edition = params.edition
        self.appcontext = params.appcontext

        # if drcp is used, use purity = NEW as the default purity for
        # standalone connections and purity = SELF for connections that belong
        # to a pool
        if description.purity == PURITY_DEFAULT \
                and self.conn_impl._drcp_enabled:
            if self.conn_impl._pool is None:
                self.purity = PURITY_NEW
            else:
                self.purity = PURITY_SELF
        else:
            self.purity = description.purity

        # set token parameters; adjust processing so that only phase two is
        # sent
        if params._token is not None \
                or params.access_token_callback is not None:
            self.token = params._get_token()
            if params._private_key is not None:
                self.private_key = params._get_private_key()
            self.function_code = TNS_FUNC_AUTH_PHASE_TWO
            self.resend = False

        # set authentication mode
        if params._new_password is None:
            self.auth_mode = TNS_AUTH_MODE_LOGON
        if params.mode & AUTH_MODE_SYSDBA:
            self.auth_mode |= TNS_AUTH_MODE_SYSDBA
        if params.mode & AUTH_MODE_SYSOPER:
            self.auth_mode |= TNS_AUTH_MODE_SYSOPER
        if params.mode & AUTH_MODE_SYSASM:
            self.auth_mode |= TNS_AUTH_MODE_SYSASM
        if params.mode & AUTH_MODE_SYSBKP:
            self.auth_mode |= TNS_AUTH_MODE_SYSBKP
        if params.mode & AUTH_MODE_SYSDGD:
            self.auth_mode |= TNS_AUTH_MODE_SYSDGD
        if params.mode & AUTH_MODE_SYSKMT:
            self.auth_mode |= TNS_AUTH_MODE_SYSKMT
        if params.mode & AUTH_MODE_SYSRAC:
            self.auth_mode |= TNS_AUTH_MODE_SYSRAC
        if self.private_key is not None:
            self.auth_mode |= TNS_AUTH_MODE_IAM_TOKEN

    cdef int _write_key_value(self, WriteBuffer buf, str key, str value,
                              uint32_t flags=0) except -1:
        cdef:
            bytes key_bytes = key.encode()
            bytes value_bytes = value.encode()
            uint32_t key_len = <uint32_t> len(key_bytes)
            uint32_t value_len = <uint32_t> len(value_bytes)
        buf.write_ub4(key_len)
        buf.write_bytes_with_length(key_bytes)
        buf.write_ub4(value_len)
        if value_len > 0:
            buf.write_bytes_with_length(value_bytes)
        buf.write_ub4(flags)

    cdef int _write_message(self, WriteBuffer buf) except -1:
        cdef:
            uint8_t has_user = 1 if self.user_bytes_len > 0 else 0
            uint32_t num_pairs

        # perform final determination of data to write
        if self.function_code == TNS_FUNC_AUTH_PHASE_ONE:
            num_pairs = 5
        elif self.change_password:
            self._encrypt_passwords()
            num_pairs = 2
        else:
            num_pairs = 4

            # token authentication
            if self.token is not None:
                num_pairs += 1

            # normal user/password authentication
            else:
                num_pairs += 2
                self.auth_mode |= TNS_AUTH_MODE_WITH_PASSWORD
                if self.verifier_type == TNS_VERIFIER_TYPE_12C:
                    num_pairs += 1
                elif self.verifier_type not in (TNS_VERIFIER_TYPE_11G_1,
                                                TNS_VERIFIER_TYPE_11G_2):
                    errors._raise_err(errors.ERR_UNSUPPORTED_VERIFIER_TYPE,
                                      verifier_type=self.verifier_type)
                self._generate_verifier()

            # determine which other key/value pairs to write
            if self.newpassword is not None:
                num_pairs += 1
                self.auth_mode |= TNS_AUTH_MODE_CHANGE_PASSWORD
            if self.proxy_user is not None:
                num_pairs += 1
            if self.conn_impl._cclass is not None:
                num_pairs += 1
            if self.purity != 0:
                num_pairs += 1
            if self.private_key is not None:
                num_pairs += 2
            if self.encoded_jdwp_data is not None:
                num_pairs += 1
            if self.edition is not None:
                num_pairs += 1
            if self.appcontext is not None:
                num_pairs += len(self.appcontext) * 3

        # write basic data to packet
        self._write_function_code(buf)
        buf.write_uint8(has_user)           # pointer (authusr)
        buf.write_ub4(self.user_bytes_len)
        buf.write_ub4(self.auth_mode)       # authentication mode
        buf.write_uint8(1)                  # pointer (authivl)
        buf.write_ub4(num_pairs)            # number of key/value pairs
        buf.write_uint8(1)                  # pointer (authovl)
        buf.write_uint8(1)                  # pointer (authovln)
        if has_user:
            buf.write_bytes_with_length(self.user_bytes)

        # write key/value pairs
        if self.function_code == TNS_FUNC_AUTH_PHASE_ONE:
            self._write_key_value(buf, "AUTH_TERMINAL", self.terminal)
            self._write_key_value(buf, "AUTH_PROGRAM_NM", self.program)
            self._write_key_value(buf, "AUTH_MACHINE", self.machine)
            self._write_key_value(buf, "AUTH_PID", _connect_constants.pid)
            self._write_key_value(buf, "AUTH_SID", self.osuser)
        else:
            if self.proxy_user is not None:
                self._write_key_value(buf, "PROXY_CLIENT_NAME",
                                      self.proxy_user)
            if self.token is not None:
                self._write_key_value(buf, "AUTH_TOKEN", self.token)
            elif not self.change_password:
                self._write_key_value(buf, "AUTH_SESSKEY", self.session_key, 1)
                if self.verifier_type == TNS_VERIFIER_TYPE_12C:
                    self._write_key_value(buf, "AUTH_PBKDF2_SPEEDY_KEY",
                                          self.speedy_key)
            if self.encoded_password is not None:
                self._write_key_value(buf, "AUTH_PASSWORD",
                                      self.encoded_password)
            if self.encoded_newpassword is not None:
                self._write_key_value(buf, "AUTH_NEWPASSWORD",
                                      self.encoded_newpassword)
            if not self.change_password:
                self._write_key_value(buf, "SESSION_CLIENT_CHARSET", "873")
                self._write_key_value(buf, "SESSION_CLIENT_DRIVER_NAME",
                                      self.driver_name)
                self._write_key_value(buf, "SESSION_CLIENT_VERSION",
                                    str(_connect_constants.full_version_num))
                self._write_key_value(buf, "AUTH_ALTER_SESSION",
                                      self._get_alter_timezone_statement(), 1)
            if self.conn_impl._cclass is not None:
                self._write_key_value(buf, "AUTH_KPPL_CONN_CLASS",
                                      self.conn_impl._cclass)
            if self.purity != 0:
                self._write_key_value(buf, "AUTH_KPPL_PURITY",
                                      str(self.purity), 1)
            if self.private_key is not None:
                date_format = "%a, %d %b %Y %H:%M:%S GMT"
                now = datetime.datetime.utcnow().strftime(date_format)
                host_info = "%s:%d" % buf._transport.get_host_info()
                header = f"date: {now}\n" + \
                         f"(request-target): {self.service_name}\n" + \
                         f"host: {host_info}"
                signature = get_signature(self.private_key, header)
                self._write_key_value(buf, "AUTH_HEADER", header)
                self._write_key_value(buf, "AUTH_SIGNATURE", signature)
            if self.encoded_jdwp_data is not None:
                self._write_key_value(buf, "AUTH_ORA_DEBUG_JDWP",
                                      self.encoded_jdwp_data)
            if self.edition is not None:
                self._write_key_value(buf, "AUTH_ORA_EDITION", self.edition)
            if self.appcontext is not None:
                # NOTE: these keys require a trailing null character as the
                # server expects it!
                for entry in self.appcontext:
                    self._write_key_value(buf, "AUTH_APPCTX_NSPACE\0", entry[0])
                    self._write_key_value(buf, "AUTH_APPCTX_ATTR\0", entry[1])
                    self._write_key_value(buf, "AUTH_APPCTX_VALUE\0", entry[2])


@cython.final
cdef class ChangePasswordMessage(AuthMessage):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.change_password = True
        self.function_code = TNS_FUNC_AUTH_PHASE_TWO
        self.user_bytes = self.conn_impl.username.encode()
        self.user_bytes_len = len(self.user_bytes)
        self.auth_mode = TNS_AUTH_MODE_WITH_PASSWORD | \
                TNS_AUTH_MODE_CHANGE_PASSWORD


@cython.final
cdef class CommitMessage(Message):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_COMMIT


@cython.final
cdef class ConnectMessage(Message):
    cdef:
        bytes connect_string_bytes
        uint16_t connect_string_len, redirect_data_len
        bint read_redirect_data_len
        Description description
        uint8_t packet_flags
        str redirect_data
        str host
        int port

    cdef int process(self, ReadBuffer buf) except -1:
        cdef:
            uint16_t protocol_version, protocol_options
            const char_type *redirect_data
            uint32_t flags = 0
            bytes db_uuid
        if buf._current_packet.packet_type == TNS_PACKET_TYPE_REDIRECT:
            if not self.read_redirect_data_len:
                buf.read_uint16(&self.redirect_data_len)
                self.read_redirect_data_len = True
            buf.wait_for_packets_sync()
            redirect_data = buf.read_raw_bytes(self.redirect_data_len)
            if self.redirect_data_len > 0:
                self.redirect_data = \
                        redirect_data[:self.redirect_data_len].decode()
            self.read_redirect_data_len = False
        elif buf._current_packet.packet_type == TNS_PACKET_TYPE_ACCEPT:
            buf.read_uint16(&protocol_version)
            # check if the protocol version supported by the database is high
            # enough; if not, reject the connection immediately
            if protocol_version < TNS_VERSION_MIN_ACCEPTED:
                errors._raise_err(errors.ERR_SERVER_VERSION_NOT_SUPPORTED)
            buf.read_uint16(&protocol_options)
            buf.skip_raw_bytes(20)
            buf.read_uint32(&buf._caps.sdu)
            if protocol_version >= TNS_VERSION_MIN_OOB_CHECK:
                buf.skip_raw_bytes(5)
                buf.read_uint32(&flags)
            buf._caps._adjust_for_protocol(protocol_version, protocol_options,
                                           flags)
            buf._transport._full_packet_size = True
        elif buf._current_packet.packet_type == TNS_PACKET_TYPE_REFUSE:
            response = self.error_info.message
            error_code = "unknown"
            error_code_int = 0
            if response is not None:
                pos = response.find("(ERR=")
                if pos > 0:
                    end_pos = response.find(")", pos)
                    if end_pos > 0:
                        error_code = response[pos + 5:end_pos]
                        error_code_int = int(error_code)
            if error_code_int == 0:
                errors._raise_err(errors.ERR_UNEXPECTED_REFUSE)
            if error_code_int == TNS_ERR_INVALID_SERVICE_NAME:
                errors._raise_err(errors.ERR_INVALID_SERVICE_NAME,
                                  service_name=self.description.service_name,
                                  host=self.host, port=self.port)
            elif error_code_int == TNS_ERR_INVALID_SID:
                errors._raise_err(errors.ERR_INVALID_SID,
                                  sid=self.description.sid,
                                  host=self.host, port=self.port)
            errors._raise_err(errors.ERR_LISTENER_REFUSED_CONNECTION,
                              error_code=error_code)

    cdef int send(self, WriteBuffer buf) except -1:
        cdef:
            uint16_t service_options = TNS_GSO_DONT_CARE
            uint32_t connect_flags_1 = 0, connect_flags_2 = 0
            uint8_t nsi_flags = \
                    TNS_NSI_SUPPORT_SECURITY_RENEG | TNS_NSI_DISABLE_NA
        if buf._caps.supports_oob:
            service_options |= TNS_GSO_CAN_RECV_ATTENTION
            connect_flags_2 |= TNS_CHECK_OOB
        buf.start_request(TNS_PACKET_TYPE_CONNECT, self.packet_flags)
        buf.write_uint16(TNS_VERSION_DESIRED)
        buf.write_uint16(TNS_VERSION_MINIMUM)
        buf.write_uint16(service_options)
        buf.write_uint16(self.description.sdu)
        buf.write_uint16(self.description.sdu)
        buf.write_uint16(TNS_PROTOCOL_CHARACTERISTICS)
        buf.write_uint16(0)                 # line turnaround
        buf.write_uint16(1)                 # value of 1
        buf.write_uint16(self.connect_string_len)
        buf.write_uint16(74)                # offset to connect data
        buf.write_uint32(0)                 # max receivable data
        buf.write_uint8(nsi_flags)
        buf.write_uint8(nsi_flags)
        buf.write_uint64(0)                 # obsolete
        buf.write_uint64(0)                 # obsolete
        buf.write_uint64(0)                 # obsolete
        buf.write_uint32(self.description.sdu)      # SDU (large)
        buf.write_uint32(self.description.sdu)      # TDU (large)
        buf.write_uint32(connect_flags_1)
        buf.write_uint32(connect_flags_2)
        if self.connect_string_len > TNS_MAX_CONNECT_DATA:
            buf.end_request()
            buf.start_request(TNS_PACKET_TYPE_DATA)
        buf.write_bytes(self.connect_string_bytes)
        buf.end_request()


@cython.final
cdef class DataTypesMessage(Message):

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        cdef uint16_t data_type, conv_data_type
        while True:
            buf.read_uint16(&data_type)
            if data_type == 0:
                break
            buf.read_uint16(&conv_data_type)
            if conv_data_type != 0:
                buf.skip_raw_bytes(4)
        if not buf._caps.supports_end_of_response:
            self.end_of_response = True

    cdef int _write_message(self, WriteBuffer buf) except -1:
        cdef:
            DataType* data_type
            int i

        # write character set and capabilities
        buf.write_uint8(TNS_MSG_TYPE_DATA_TYPES)
        buf.write_uint16(TNS_CHARSET_UTF8, BYTE_ORDER_LSB)
        buf.write_uint16(TNS_CHARSET_UTF8, BYTE_ORDER_LSB)
        buf.write_uint8(TNS_ENCODING_MULTI_BYTE | TNS_ENCODING_CONV_LENGTH)
        buf.write_bytes_with_length(bytes(buf._caps.compile_caps))
        buf.write_bytes_with_length(bytes(buf._caps.runtime_caps))

        # write data types
        i = 0
        while True:
            data_type = &DATA_TYPES[i]
            if data_type.data_type == 0:
                break
            i += 1
            buf.write_uint16(data_type.data_type)
            buf.write_uint16(data_type.conv_data_type)
            buf.write_uint16(data_type.representation)
            buf.write_uint16(0)
        buf.write_uint16(0)


@cython.final
cdef class EndPipelineMessage(Message):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_PIPELINE_END

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Write the message to the buffer.
        """
        self._write_function_code(buf)
        buf.write_ub4(0)                    # ID (unused)


@cython.final
cdef class ExecuteMessage(MessageWithData):

    cdef int _write_execute_message(self, WriteBuffer buf) except -1:
        """
        Write the message for a full execute.
        """
        cdef:
            uint32_t options, dml_options = 0, num_params = 0, num_iters = 1
            Statement stmt = self.cursor_impl._statement
            BaseThinCursorImpl cursor_impl = self.cursor_impl
            list params = stmt._bind_info_list

        # determine the options to use for the execute
        options = 0
        if not stmt._requires_define and not self.parse_only \
                and params is not None:
            num_params = <uint32_t> len(params)
        if stmt._requires_define:
            options |= TNS_EXEC_OPTION_DEFINE
        elif not self.parse_only and stmt._sql is not None:
            dml_options = TNS_EXEC_OPTION_IMPLICIT_RESULTSET
            options |= TNS_EXEC_OPTION_EXECUTE
        if stmt._cursor_id == 0 or stmt._is_ddl:
            options |= TNS_EXEC_OPTION_PARSE
        if stmt._is_query:
            if self.parse_only:
                options |= TNS_EXEC_OPTION_DESCRIBE
            else:
                if stmt._cursor_id == 0 or stmt._requires_define:
                    num_iters = self.cursor_impl.prefetchrows
                else:
                    num_iters = self.cursor_impl.arraysize
                self.cursor_impl._set_fetch_array_size(num_iters)
                if num_iters > 0 and not stmt._no_prefetch:
                    options |= TNS_EXEC_OPTION_FETCH
        if not stmt._is_plsql and not self.parse_only:
            options |= TNS_EXEC_OPTION_NOT_PLSQL
        elif stmt._is_plsql and num_params > 0:
            options |= TNS_EXEC_OPTION_PLSQL_BIND
        if num_params > 0:
            options |= TNS_EXEC_OPTION_BIND
        if self.batcherrors:
            options |= TNS_EXEC_OPTION_BATCH_ERRORS
        if self.arraydmlrowcounts:
            dml_options = TNS_EXEC_OPTION_DML_ROWCOUNTS
        if self.conn_impl.autocommit and not self.parse_only:
            options |= TNS_EXEC_OPTION_COMMIT

        # write piggybacks, if needed
        self._write_piggybacks(buf)

        # write body of message
        self._write_function_code(buf)
        buf.write_ub4(options)              # execute options
        buf.write_ub4(stmt._cursor_id)      # cursor id
        if stmt._cursor_id == 0 or stmt._is_ddl:
            buf.write_uint8(1)              # pointer (cursor id)
            buf.write_ub4(stmt._sql_length)
        else:
            buf.write_uint8(0)              # pointer (cursor id)
            buf.write_ub4(0)
        buf.write_uint8(1)                  # pointer (vector)
        buf.write_ub4(13)                   # al8i4 array length
        buf.write_uint8(0)                  # pointer (al8o4)
        buf.write_uint8(0)                  # pointer (al8o4l)
        buf.write_ub4(0)                    # prefetch buffer size
        buf.write_ub4(num_iters)            # prefetch number of rows
        buf.write_ub4(TNS_MAX_LONG_LENGTH)  # maximum long size
        if num_params == 0:
            buf.write_uint8(0)              # pointer (binds)
            buf.write_ub4(0)                # number of binds
        else:
            buf.write_uint8(1)              # pointer (binds)
            buf.write_ub4(num_params)       # number of binds
        buf.write_uint8(0)                  # pointer (al8app)
        buf.write_uint8(0)                  # pointer (al8txn)
        buf.write_uint8(0)                  # pointer (al8txl)
        buf.write_uint8(0)                  # pointer (al8kv)
        buf.write_uint8(0)                  # pointer (al8kvl)
        if stmt._requires_define:
            buf.write_uint8(1)              # pointer (al8doac)
            buf.write_ub4(len(self.cursor_impl.fetch_vars))
                                            # number of defines
        else:
            buf.write_uint8(0)
            buf.write_ub4(0)
        buf.write_ub4(0)                    # registration id
        buf.write_uint8(0)                  # pointer (al8objlist)
        buf.write_uint8(1)                  # pointer (al8objlen)
        buf.write_uint8(0)                  # pointer (al8blv)
        buf.write_ub4(0)                    # al8blvl
        buf.write_uint8(0)                  # pointer (al8dnam)
        buf.write_ub4(0)                    # al8dnaml
        buf.write_ub4(0)                    # al8regid_msb
        if self.arraydmlrowcounts:
            buf.write_uint8(1)              # pointer (al8pidmlrc)
            buf.write_ub4(self.num_execs)   # al8pidmlrcbl
            buf.write_uint8(1)              # pointer (al8pidmlrcl)
        else:
            buf.write_uint8(0)              # pointer (al8pidmlrc)
            buf.write_ub4(0)                # al8pidmlrcbl
            buf.write_uint8(0)              # pointer (al8pidmlrcl)
        if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2:
            buf.write_uint8(0)                  # pointer (al8sqlsig)
            buf.write_ub4(0)                    # SQL signature length
            buf.write_uint8(0)                  # pointer (SQL ID)
            buf.write_ub4(0)                    # allocated size of SQL ID
            buf.write_uint8(0)                  # pointer (length of SQL ID)
            if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1:
                buf.write_uint8(0)              # pointer (chunk ids)
                buf.write_ub4(0)                # number of chunk ids
        if stmt._cursor_id == 0 or stmt._is_ddl:
            if stmt._sql_bytes is None:
                errors._raise_err(errors.ERR_INVALID_REF_CURSOR)
            buf.write_bytes_with_length(stmt._sql_bytes)
            buf.write_ub4(1)                # al8i4[0] parse
        else:
            buf.write_ub4(0)                # al8i4[0] parse
        if stmt._is_query:
            if stmt._cursor_id == 0:
                buf.write_ub4(0)            # al8i4[1] execution count
            else:
                buf.write_ub4(num_iters)
        else:
            buf.write_ub4(self.num_execs)   # al8i4[1] execution count
        buf.write_ub4(0)                    # al8i4[2]
        buf.write_ub4(0)                    # al8i4[3]
        buf.write_ub4(0)                    # al8i4[4]
        buf.write_ub4(0)                    # al8i4[5] SCN (part 1)
        buf.write_ub4(0)                    # al8i4[6] SCN (part 2)
        buf.write_ub4(stmt._is_query)       # al8i4[7] is query
        buf.write_ub4(0)                    # al8i4[8]
        buf.write_ub4(dml_options)          # al8i4[9] DML row counts/implicit
        buf.write_ub4(0)                    # al8i4[10]
        buf.write_ub4(0)                    # al8i4[11]
        buf.write_ub4(0)                    # al8i4[12]
        if stmt._requires_define:
            self._write_column_metadata(buf, self.cursor_impl.fetch_var_impls)
        elif num_params > 0:
            self._write_bind_params(buf, params)

    cdef int _write_reexecute_message(self, WriteBuffer buf) except -1:
        """
        Write the message for a re-execute.
        """
        cdef:
            uint32_t i, exec_flags_1 = 0, exec_flags_2 = 0, num_iters
            Statement stmt = self.cursor_impl._statement
            list params = stmt._bind_info_list
            BindInfo info

        if params:
            if not stmt._is_query and not stmt._is_returning:
                self.out_var_impls = [info._bind_var_impl \
                                      for info in params \
                                      if info.bind_dir != TNS_BIND_DIR_INPUT]
            params = [info for info in params \
                      if info.bind_dir != TNS_BIND_DIR_OUTPUT \
                      and not info._is_return_bind]
        if self.function_code == TNS_FUNC_REEXECUTE_AND_FETCH:
            exec_flags_1 |= TNS_EXEC_OPTION_EXECUTE
            num_iters = self.cursor_impl.prefetchrows
            self.cursor_impl._set_fetch_array_size(num_iters)
        else:
            if self.conn_impl.autocommit:
                exec_flags_2 |= TNS_EXEC_OPTION_COMMIT_REEXECUTE
            num_iters = self.num_execs

        self._write_piggybacks(buf)
        self._write_function_code(buf)
        buf.write_ub4(stmt._cursor_id)
        buf.write_ub4(num_iters)
        buf.write_ub4(exec_flags_1)
        buf.write_ub4(exec_flags_2)
        if params:
            for i in range(self.num_execs):
                buf.write_uint8(TNS_MSG_TYPE_ROW_DATA)
                self._write_bind_params_row(buf, params, i)

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Write the execute message to the buffer. Two types of execute messages
        are possible: one for a full execute and the second, simpler message,
        for when an existing cursor is being re-executed. A full execute is
        required under the following circumstances:
            - the statement has never been executed
            - the statement refers to a REF cursor (no sql is defined)
            - prefetch is not possible (LOB columns fetched)
            - bind metadata has changed
            - parse is being performed
            - define is being performed
            - DDL is being executed
            - batch errors mode is enabled
        """
        cdef:
            Statement stmt = self.cursor_impl._statement
        if stmt._cursor_id == 0 or not stmt._executed \
                or stmt._sql is None \
                or stmt._no_prefetch \
                or stmt._binds_changed \
                or self.parse_only \
                or stmt._requires_define \
                or stmt._is_ddl \
                or self.batcherrors:
            self.function_code = TNS_FUNC_EXECUTE
            self._write_execute_message(buf)
        elif stmt._is_query and self.cursor_impl.prefetchrows > 0:
            self.function_code = TNS_FUNC_REEXECUTE_AND_FETCH
            self._write_reexecute_message(buf)
        else:
            self.function_code = TNS_FUNC_REEXECUTE
            self._write_reexecute_message(buf)
        stmt._binds_changed = False

    cdef int process(self, ReadBuffer buf) except -1:
        """
        Runs after the database response has been processed. If the statement
        executed requires define and is not a REF cursor (which would already
        have performed the define during its execute), then mark the message as
        needing to be resent. If this is after the second time the message has
        been sent, mark the statement as no longer needing a define (since this
        only needs to happen once).
        """
        cdef Statement stmt = self.cursor_impl._statement
        MessageWithData.process(self, buf)
        if self.error_occurred and self.error_info.pos == 0 and stmt._is_plsql:
            self.error_info.pos = self.error_info.rowcount + self.offset
        if not self.parse_only:
            stmt._executed = True
        if stmt._requires_define and stmt._sql is not None:
            if self.resend:
                stmt._requires_define = False
            else:
                self.resend = True


@cython.final
cdef class FetchMessage(MessageWithData):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_FETCH

    cdef int _write_message(self, WriteBuffer buf) except -1:
        self.cursor_impl._set_fetch_array_size(self.cursor_impl.arraysize)
        self._write_function_code(buf)
        if self.cursor_impl._statement._cursor_id == 0:
            errors._raise_err(errors.ERR_CURSOR_HAS_BEEN_CLOSED)
        buf.write_ub4(self.cursor_impl._statement._cursor_id)
        buf.write_ub4(self.cursor_impl._fetch_array_size)


@cython.final
cdef class LobOpMessage(Message):
    cdef:
        uint32_t operation
        BaseThinLobImpl source_lob_impl
        BaseThinLobImpl dest_lob_impl
        uint64_t source_offset
        uint64_t dest_offset
        int64_t amount
        bint send_amount
        bint bool_flag
        object data

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_LOB_OP

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        cdef:
            const char* encoding
            const char_type *ptr
            ssize_t num_bytes
        if message_type == TNS_MSG_TYPE_LOB_DATA:
            buf.read_raw_bytes_and_length(&ptr, &num_bytes)
            if self.source_lob_impl.dbtype._ora_type_num in \
                    (TNS_DATA_TYPE_BLOB, TNS_DATA_TYPE_BFILE):
                self.data = ptr[:num_bytes]
            else:
                encoding = self.source_lob_impl._get_encoding()
                self.data = ptr[:num_bytes].decode(encoding)
        else:
            Message._process_message(self, buf, message_type)

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        cdef:
            cdef const char_type *ptr
            ssize_t num_bytes
            uint8_t temp8
        if self.source_lob_impl is not None:
            num_bytes = len(self.source_lob_impl._locator)
            ptr = buf.read_raw_bytes(num_bytes)
            self.source_lob_impl._locator = ptr[:num_bytes]
        if self.dest_lob_impl is not None:
            num_bytes = len(self.dest_lob_impl._locator)
            ptr = buf.read_raw_bytes(num_bytes)
            self.dest_lob_impl._locator = ptr[:num_bytes]
        if self.operation == TNS_LOB_OP_CREATE_TEMP:
            buf.skip_ub2()                  # skip character set
            buf.skip_raw_bytes(3)           # skip trailing flags, amount
        elif self.send_amount:
            buf.read_sb8(&self.amount)
        if self.operation in (TNS_LOB_OP_IS_OPEN,
                              TNS_LOB_OP_FILE_EXISTS,
                              TNS_LOB_OP_FILE_ISOPEN):
            buf.read_ub1(&temp8)
            self.bool_flag = temp8 > 0

    cdef int _write_message(self, WriteBuffer buf) except -1:
        cdef int i
        self._write_function_code(buf)
        if self.source_lob_impl is None:
            buf.write_uint8(0)              # source pointer
            buf.write_ub4(0)                # source length
        else:
            buf.write_uint8(1)              # source pointer
            buf.write_ub4(len(self.source_lob_impl._locator))
        if self.dest_lob_impl is None:
            buf.write_uint8(0)              # dest pointer
            buf.write_ub4(0)                # dest length
        else:
            buf.write_uint8(1)              # dest pointer
            buf.write_ub4(len(self.dest_lob_impl._locator))
        buf.write_ub4(0)                    # short source offset
        buf.write_ub4(0)                    # short dest offset
        if self.operation == TNS_LOB_OP_CREATE_TEMP:
            buf.write_uint8(1)              # pointer (character set)
        else:
            buf.write_uint8(0)              # pointer (character set)
        buf.write_uint8(0)                  # pointer (short amount)
        if self.operation in (TNS_LOB_OP_CREATE_TEMP,
                              TNS_LOB_OP_IS_OPEN,
                              TNS_LOB_OP_FILE_EXISTS,
                              TNS_LOB_OP_FILE_ISOPEN):
            buf.write_uint8(1)              # pointer (NULL LOB)
        else:
            buf.write_uint8(0)              # pointer (NULL LOB)
        buf.write_ub4(self.operation)
        buf.write_uint8(0)                  # pointer (SCN array)
        buf.write_uint8(0)                  # SCN array length
        buf.write_ub8(self.source_offset)
        buf.write_ub8(self.dest_offset)
        if self.send_amount:
            buf.write_uint8(1)              # pointer (amount)
        else:
            buf.write_uint8(0)              # pointer (amount)
        for i in range(3):                  # array LOB (not used)
            buf.write_uint16(0)
        if self.source_lob_impl is not None:
            buf.write_bytes(self.source_lob_impl._locator)
        if self.dest_lob_impl is not None:
            buf.write_bytes(self.dest_lob_impl._locator)
        if self.operation == TNS_LOB_OP_CREATE_TEMP:
            if self.source_lob_impl.dbtype._csfrm == CS_FORM_NCHAR:
                buf._caps._check_ncharset_id()
                buf.write_ub4(TNS_CHARSET_UTF16)
            else:
                buf.write_ub4(TNS_CHARSET_UTF8)
        if self.data is not None:
            buf.write_uint8(TNS_MSG_TYPE_LOB_DATA)
            buf.write_bytes_with_length(self.data)
        if self.send_amount:
            buf.write_ub8(self.amount)      # LOB amount


@cython.final
cdef class LogoffMessage(Message):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_LOGOFF


@cython.final
cdef class PingMessage(Message):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_PING


@cython.final
cdef class ProtocolMessage(Message):
    cdef:
        uint8_t server_version
        uint8_t server_flags
        bytes server_compile_caps
        bytes server_runtime_caps
        bytes server_banner

    cdef int _write_message(self, WriteBuffer buf) except -1:
        buf.write_uint8(TNS_MSG_TYPE_PROTOCOL)
        buf.write_uint8(6)                  # protocol version (8.1 and higher)
        buf.write_uint8(0)                  # "array" terminator
        buf.write_str(DRIVER_NAME)
        buf.write_uint8(0)                  # NULL terminator

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        if message_type == TNS_MSG_TYPE_PROTOCOL:
            self._process_protocol_info(buf)
            if not buf._caps.supports_end_of_response:
                self.end_of_response = True
        else:
            Message._process_message(self, buf, message_type)

    cdef int _process_protocol_info(self, ReadBuffer buf) except -1:
        """
        Processes the response to the protocol request.
        """
        cdef:
            uint16_t num_elem, fdo_length
            Capabilities caps = buf._caps
            const char_type *fdo
            bytearray temp_array
            ssize_t ix
        buf.read_ub1(&self.server_version)
        buf.skip_ub1()                      # skip zero byte
        self.server_banner = buf.read_null_terminated_bytes()
        buf.read_uint16(&caps.charset_id, BYTE_ORDER_LSB)
        buf.read_ub1(&self.server_flags)
        buf.read_uint16(&num_elem, BYTE_ORDER_LSB)
        if num_elem > 0:                    # skip elements
            buf.skip_raw_bytes(num_elem * 5)
        buf.read_uint16(&fdo_length)
        fdo = buf.read_raw_bytes(fdo_length)
        ix = 6 + fdo[5] + fdo[6]
        caps.ncharset_id = (fdo[ix + 3] << 8) + fdo[ix + 4]
        self.server_compile_caps = buf.read_bytes()
        if self.server_compile_caps is not None:
            temp_array = bytearray(self.server_compile_caps)
            caps._adjust_for_server_compile_caps(temp_array)
            if caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1:
                self.conn_impl._oson_max_fname_size = 65535
        self.server_runtime_caps = buf.read_bytes()
        if self.server_runtime_caps is not None:
            temp_array = bytearray(self.server_runtime_caps)
            caps._adjust_for_server_runtime_caps(temp_array)


@cython.final
cdef class FastAuthMessage(Message):
    cdef:
        DataTypesMessage data_types_message
        ProtocolMessage protocol_message
        AuthMessage auth_message

    cdef int _process_message(self, ReadBuffer buf,
                              uint8_t message_type) except -1:
        """
        Processes the messages returned from the server response.
        """
        if message_type == TNS_MSG_TYPE_PROTOCOL:
            ProtocolMessage._process_message(self.protocol_message, buf,
                                             message_type)
        elif message_type == TNS_MSG_TYPE_DATA_TYPES:
            DataTypesMessage._process_message(self.data_types_message, buf,
                                              message_type)
        else:
            AuthMessage._process_message(self.auth_message, buf, message_type)
            self.end_of_response = self.auth_message.end_of_response

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Writes the message to the buffer. This includes not just this message
        but also the protocol, data types and auth messages. This reduces the
        number of round-trips to the database and thereby increases
        performance.
        """
        buf.write_uint8(TNS_MSG_TYPE_FAST_AUTH)
        buf.write_uint8(1)                  # fast auth version
        buf.write_uint8(TNS_SERVER_CONVERTS_CHARS)  # flag 1
        buf.write_uint8(0)                  # flag 2
        ProtocolMessage._write_message(self.protocol_message, buf)
        buf.write_uint16(0)                 # server charset (unused)
        buf.write_uint8(0)                  # server charset flag (unused)
        buf.write_uint16(0)                 # server ncharset (unused)
        buf._caps.ttc_field_version = TNS_CCAP_FIELD_VERSION_19_1_EXT_1
        buf.write_uint8(buf._caps.ttc_field_version)
        DataTypesMessage._write_message(self.data_types_message, buf)
        AuthMessage._write_message(self.auth_message, buf)
        buf._caps.ttc_field_version = TNS_CCAP_FIELD_VERSION_MAX


@cython.final
cdef class RollbackMessage(Message):

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_ROLLBACK


@cython.final
cdef class SessionReleaseMessage(Message):

    cdef:
        uint32_t release_mode

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.message_type = TNS_MSG_TYPE_ONEWAY_FN
        self.function_code = TNS_FUNC_SESSION_RELEASE

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Write the message for a DRCP session release.
        """
        self._write_function_code(buf)
        buf.write_uint8(0)                  # pointer (tag name)
        buf.write_uint8(0)                  # tag name length
        buf.write_ub4(self.release_mode)    # mode


@cython.final
cdef class TransactionChangeStateMessage(Message):
    """
    Used for two-phase commit (TPC) transaction change state: commit, rollback,
    forget, etc.
    """
    cdef:
        uint32_t operation, state, flags
        bytes context
        object xid

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_TPC_TXN_CHANGE_STATE

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        """
        Process the parameters returned by the database.
        """
        buf.read_ub4(&self.state)

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Writes the message to the database.
        """
        cdef:
            bytes global_transaction_id, branch_qualifier, xid_bytes
            uint32_t format_id = 0

        # acquire data to send to the server
        if self.xid is not None:
            format_id = self.xid[0]
            global_transaction_id = self.xid[1] \
                    if isinstance(self.xid[1], bytes) \
                    else self.xid[1].encode()
            branch_qualifier = self.xid[2] \
                    if isinstance(self.xid[2], bytes) \
                    else self.xid[2].encode()
            xid_bytes = global_transaction_id + branch_qualifier
            xid_bytes += bytes(128 - len(xid_bytes))

        self._write_function_code(buf)
        buf.write_ub4(self.operation)
        if self.context is not None:
            buf.write_uint8(1)              # pointer (context)
            buf.write_ub4(len(self.context))
        else:
            buf.write_uint8(0)              # pointer (context)
            buf.write_ub4(0)                # context length
        if self.xid is not None:
            buf.write_ub4(format_id)
            buf.write_ub4(len(global_transaction_id))
            buf.write_ub4(len(branch_qualifier))
            buf.write_uint8(1)              # pointer (xid)
            buf.write_ub4(len(xid_bytes))
        else:
            buf.write_ub4(0)                # format id
            buf.write_ub4(0)                # global transaction id length
            buf.write_ub4(0)                # branch qualifier length
            buf.write_uint8(0)              # pointer (xid)
            buf.write_ub4(0)                # XID length
        buf.write_ub4(0)                    # timeout
        buf.write_ub4(self.state)
        buf.write_uint8(1)                  # pointer (out state)
        buf.write_ub4(self.flags)
        if self.context is not None:
            buf.write_bytes(self.context)
        if self.xid is not None:
            buf.write_bytes(xid_bytes)


@cython.final
cdef class TransactionSwitchMessage(Message):
    """
    Used for two-phase commit (TPC) transaction start, attach and detach.
    """
    cdef:
        uint32_t operation, flags, timeout, application_value
        bytes context
        object xid

    cdef int _initialize_hook(self) except -1:
        """
        Perform initialization.
        """
        self.function_code = TNS_FUNC_TPC_TXN_SWITCH

    cdef int _process_return_parameters(self, ReadBuffer buf) except -1:
        """
        Process the parameters returned by the database.
        """
        cdef:
            const char_type* ptr
            uint16_t context_len
        buf.read_ub4(&self.application_value)
        buf.read_ub2(&context_len)
        ptr = buf.read_raw_bytes(context_len)
        self.context = ptr[:context_len]

    cdef int _write_message(self, WriteBuffer buf) except -1:
        """
        Writes the message to the database.
        """
        cdef:
            bytes global_transaction_id, branch_qualifier, xid_bytes
            bytes internal_name = None, external_name = None
            uint32_t format_id = 0

        # acquire data to send to the server
        if self.xid is not None:
            format_id = self.xid[0]
            global_transaction_id = self.xid[1] \
                    if isinstance(self.xid[1], bytes) \
                    else self.xid[1].encode()
            branch_qualifier = self.xid[2] \
                    if isinstance(self.xid[2], bytes) \
                    else self.xid[2].encode()
            xid_bytes = global_transaction_id + branch_qualifier
            xid_bytes += bytes(128 - len(xid_bytes))
        if self.conn_impl._internal_name is not None:
            internal_name = self.conn_impl._internal_name.encode()
        if self.conn_impl._external_name is not None:
            external_name = self.conn_impl._external_name.encode()

        # write message
        self._write_function_code(buf)
        buf.write_ub4(self.operation)
        if self.context is not None:
            buf.write_uint8(1)              # pointer (transaction context)
            buf.write_ub4(len(self.context))
        else:
            buf.write_uint8(0)              # pointer (transaction context)
            buf.write_ub4(0)                # transaction context length
        if self.xid is not None:
            buf.write_ub4(format_id)
            buf.write_ub4(len(global_transaction_id))
            buf.write_ub4(len(branch_qualifier))
            buf.write_uint8(1)              # pointer (XID)
            buf.write_ub4(len(xid_bytes))
        else:
            buf.write_ub4(0)                # format id
            buf.write_ub4(0)                # global transaction id length
            buf.write_ub4(0)                # branch qualifier length
            buf.write_uint8(0)              # pointer (XID)
            buf.write_ub4(0)                # XID length
        buf.write_ub4(self.flags)
        buf.write_ub4(self.timeout)
        buf.write_uint8(1)                  # pointer (application value)
        buf.write_uint8(1)                  # pointer (return context)
        buf.write_uint8(1)                  # pointer (return context length)
        if internal_name is not None:
            buf.write_uint8(1)              # pointer (internal name)
            buf.write_ub4(len(internal_name))
        else:
            buf.write_uint8(0)              # pointer (internal name)
            buf.write_ub4(0)                # length of internal name
        if external_name is not None:
            external_name = self.conn_impl._external_name.encode()
            buf.write_uint8(1)              # pointer (external name)
            buf.write_ub4(len(external_name))
        else:
            buf.write_uint8(0)              # pointer (external name)
            buf.write_ub4(0)                # length of external name
        if self.context is not None:
            buf.write_bytes(self.context)
        if self.xid is not None:
            buf.write_bytes(xid_bytes)
        buf.write_ub4(self.application_value)
        if internal_name is not None:
            buf.write_bytes(internal_name)
        if external_name is not None:
            buf.write_bytes(external_name)
