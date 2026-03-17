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
# connection.pyx
#
# Cython file defining the thin Connection implementation class (embedded in
# thin_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseThinConnImpl(BaseConnImpl):

    cdef:
        StatementCache _statement_cache
        BaseProtocol _protocol
        uint32_t _session_id
        uint16_t _serial_num
        str _action
        bint _action_modified
        str _dbop
        bint _dbop_modified
        str _client_info
        bint _client_info_modified
        str _client_identifier
        bint _client_identifier_modified
        str _module
        bint _module_modified
        BaseThinPoolImpl _pool
        bytes _ltxid
        str _current_schema
        bint _current_schema_modified
        uint8_t _max_identifier_length
        uint32_t _max_open_cursors
        str _db_domain
        str _db_name
        str _edition
        str _instance_name
        str _internal_name
        str _external_name
        str _service_name
        bint _drcp_enabled
        bint _drcp_establish_session
        double _time_in_pool
        list _temp_lobs_to_close
        uint32_t _temp_lobs_total_size
        uint32_t _call_timeout
        str _cclass
        int _dbobject_type_cache_num
        bytes _combo_key
        str _connection_id
        bint _is_pool_extra
        bytes _transaction_context
        uint8_t pipeline_mode

    def __init__(self, str dsn, ConnectParamsImpl params):
        if not HAS_CRYPTOGRAPHY:
            errors._raise_err(errors.ERR_NO_CRYPTOGRAPHY_PACKAGE)
        BaseConnImpl.__init__(self, dsn, params)
        self.thin = True

    cdef int _check_tpc_commit_state(self, uint32_t state,
                                     bint one_phase) except -1:
        """
        Check the state returned by the tpc_commit() call.
        """
        if one_phase and state not in (TNS_TPC_TXN_STATE_READ_ONLY,
                                       TNS_TPC_TXN_STATE_COMMITTED) \
                or not one_phase and state != TNS_TPC_TXN_STATE_FORGOTTEN:
            errors._raise_err(errors.ERR_UNKNOWN_TRANSACTION_STATE,
                              state=state)
        self._transaction_context = None

    cdef BaseThinLobImpl _create_lob_impl(self, DbType dbtype,
                                          bytes locator=None):
        """
        Create and return a LOB implementation object.
        """
        cdef BaseThinLobImpl lob_impl
        if self._protocol._transport._is_async:
            lob_impl = AsyncThinLobImpl.__new__(AsyncThinLobImpl)
        else:
            lob_impl = ThinLobImpl.__new__(ThinLobImpl)
        lob_impl._conn_impl = self
        lob_impl.dbtype = dbtype
        lob_impl._locator = locator
        return lob_impl

    cdef Message _create_message(self, type typ):
        """
        Creates a message object that is used to send a request to the database
        and receive back its response.
        """
        cdef Message message
        message = typ.__new__(typ)
        message._initialize(self)
        return message

    cdef TransactionChangeStateMessage _create_tpc_commit_message(
            self, object xid, bint one_phase
    ):
        """
        Creates a two-phase commit message suitable for committing a
        transaction.
        """
        cdef TransactionChangeStateMessage message
        message = self._create_message(TransactionChangeStateMessage)
        message.operation = TNS_TPC_TXN_COMMIT
        message.state = TNS_TPC_TXN_STATE_READ_ONLY if one_phase \
                else TNS_TPC_TXN_STATE_COMMITTED
        message.xid = xid
        message.context = self._transaction_context
        return message

    cdef Message _create_tpc_rollback_message(self, object xid=None):
        """
        Creates a two-phase commit rollback message suitable for use in both
        the close() method and explicitly by the user.
        """
        cdef TransactionChangeStateMessage message
        message = self._create_message(TransactionChangeStateMessage)
        message.operation = TNS_TPC_TXN_ABORT
        message.state = TNS_TPC_TXN_STATE_ABORTED
        message.xid = xid
        message.context = self._transaction_context
        return message

    cdef int _force_close(self) except -1:
        self._pool = None
        if self._dbobject_type_cache_num > 0:
            remove_dbobject_type_cache(self._dbobject_type_cache_num)
            self._dbobject_type_cache_num = 0
        self._protocol._force_close()

    cdef Statement _get_statement(self, str sql = None,
                                  bint cache_statement = False):
        """
        Get a statement from the statement cache, or prepare a new statement
        for use.
        """
        return self._statement_cache.get_statement(
            sql, cache_statement, self._drcp_establish_session
        )

    cdef int _post_connect_phase_one(self, Description description,
                                     ConnectParamsImpl params) except -1:
        """
        Called after the connection has been partially established to perform
        common tasks.
        """
        self._drcp_enabled = description.server_type == "pooled"
        if self._cclass is None:
            self._cclass = description.cclass
        if self._cclass is None and self._pool is not None \
                and self._drcp_enabled:
            gen_uuid = uuid.uuid4()
            self._cclass = f"DPY:{base64.b64encode(gen_uuid.bytes).decode()}"
            params._default_description.cclass = self._cclass

    cdef int _post_connect_phase_two(self, ConnectParamsImpl params) except -1:
        """
        Called after the connection has been fully established to perform
        common tasks.
        """
        self._statement_cache = StatementCache.__new__(StatementCache)
        self._statement_cache.initialize(params.stmtcachesize,
                                         self._max_open_cursors)
        self._dbobject_type_cache_num = create_new_dbobject_type_cache(self)
        self.invoke_session_callback = True

    cdef int _pre_connect(self, ConnectParamsImpl params) except -1:
        """
        Called before the connection is established to perform common tasks.
        """
        params._check_credentials()
        self._connection_id = base64.b64encode(secrets.token_bytes(16)).decode()

    cdef int _return_statement(self, Statement statement) except -1:
        """
        Return the statement to the statement cache, if applicable.
        """
        self._statement_cache.return_statement(statement)

    def cancel(self):
        self._protocol._break_external()

    def get_call_timeout(self):
        return self._call_timeout

    def get_current_schema(self):
        return self._current_schema

    def get_db_domain(self):
        if self._db_domain:
            return self._db_domain

    def get_db_name(self):
        return self._db_name

    def get_session_id(self):
        return self._session_id

    def get_serial_num(self):
        return self._serial_num

    def get_edition(self):
        return self._edition

    def get_external_name(self):
        return self._external_name

    def get_instance_name(self):
        return self._instance_name

    def get_internal_name(self):
        return self._internal_name

    def get_is_healthy(self):
        return self._protocol._transport is not None \
                and self._protocol._read_buf._pending_error_num == 0

    def get_ltxid(self):
        return self._ltxid or b''

    def get_max_identifier_length(self):
        return self._max_identifier_length

    def get_max_open_cursors(self):
        return self._max_open_cursors

    def get_sdu(self):
        return self._protocol._caps.sdu

    def get_service_name(self):
        return self._service_name

    def get_stmt_cache_size(self):
        return self._statement_cache._max_size

    def get_transaction_in_progress(self):
        return self._protocol._txn_in_progress

    def get_type(self, object conn, str name):
        cdef ThinDbObjectTypeCache cache = \
                get_dbobject_type_cache(self._dbobject_type_cache_num)
        return cache.get_type(conn, name)

    def ping(self):
        cdef Message message
        message = self._create_message(PingMessage)
        self._protocol._process_single_message(message)

    def rollback(self):
        cdef Message message
        message = self._create_message(RollbackMessage)
        self._protocol._process_single_message(message)

    def set_action(self, str value):
        self._action = value
        self._action_modified = True

    def set_client_identifier(self, str value):
        self._client_identifier = value
        self._client_identifier_modified = True

    def set_client_info(self, str value):
        self._client_info = value
        self._client_info_modified = True

    def set_current_schema(self, value):
        self._current_schema = value
        self._current_schema_modified = True

    def set_dbop(self, str value):
        self._dbop = value
        self._dbop_modified = True

    def set_external_name(self, value):
        self._external_name = value

    def set_internal_name(self, value):
        self._internal_name = value

    def set_module(self, str value):
        self._module = value
        self._module_modified = True
        # setting the module by itself results in an error so always force the
        # action to be set as well (which eliminates this error)
        self._action_modified = True

    def set_stmt_cache_size(self, uint32_t value):
        self._statement_cache.resize(value)


cdef class ThinConnImpl(BaseThinConnImpl):

    def __init__(self, str dsn, ConnectParamsImpl params):
        BaseThinConnImpl.__init__(self, dsn, params)
        self._protocol = Protocol()

    cdef int _connect_with_address(self, Address address,
                                   Description description,
                                   ConnectParamsImpl params,
                                   str connect_string,
                                   bint raise_exception) except -1:
        """
        Internal method used for connecting with the given description and
        address.
        """
        cdef Protocol protocol = <Protocol> self._protocol
        try:
            protocol._connect_phase_one(self, params, description,
                                        address, connect_string)
        except (exceptions.DatabaseError, socket.gaierror, OSError) as e:
            if raise_exception:
                errors._raise_err(errors.ERR_CONNECTION_FAILED, cause=e,
                                  connection_id=description.connection_id)
            return 0
        except Exception as e:
            errors._raise_err(errors.ERR_CONNECTION_FAILED, cause=e,
                              connection_id=description.connection_id)
        self._post_connect_phase_one(description, params)
        protocol._connect_phase_two(self, description, params)

    cdef int _connect_with_description(self, Description description,
                                       ConnectParamsImpl params,
                                       bint final_desc) except -1:
        """
        Internal method used for connecting with the given description. Retry
        connecting to the socket if an attempt fails and retry_count is
        specified in the connect string.
        """
        cdef:
            uint32_t i, j, k, num_attempts, num_lists, num_addresses
            AddressList address_list
            bint raise_exc = False
            str connect_string
            Address address
        num_lists = len(description.active_children)
        num_attempts = description.retry_count + 1
        connect_string = _get_connect_data(description, self._connection_id, params)
        for i in range(num_attempts):
            for j, address_list in enumerate(description.active_children):
                num_addresses = len(address_list.active_children)
                for k, address in enumerate(address_list.active_children):
                    if final_desc:
                        raise_exc = i == num_attempts - 1 \
                                and j == num_lists - 1 \
                                and k == num_addresses - 1
                    self._connect_with_address(address, description, params,
                                               connect_string, raise_exc)
                    if not self._protocol._in_connect:
                        return 0
            time.sleep(description.retry_delay)

    cdef int _connect_with_params(self, ConnectParamsImpl params) except -1:
        """
        Internal method used for connecting with the given parameters.
        """
        cdef:
            DescriptionList description_list = params.description_list
            ssize_t i, num_descriptions
            Description description
            bint final_desc
        description_list._set_active_children()
        num_descriptions = len(description_list.active_children)
        for i, description in enumerate(description_list.active_children):
            final_desc = (i == num_descriptions - 1)
            self._connect_with_description(description, params, final_desc)
            if not self._protocol._in_connect:
                break

    cdef BaseCursorImpl _create_cursor_impl(self):
        """
        Internal method for creating an empty cursor implementation object.
        """
        return ThinCursorImpl.__new__(ThinCursorImpl, self)

    def change_password(self, str old_password, str new_password):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            ChangePasswordMessage message
        message = self._create_message(ChangePasswordMessage)
        message.password = old_password.encode()
        message.newpassword = new_password.encode()
        protocol._process_single_message(message)

    def close(self, bint in_del=False):
        cdef Protocol protocol = <Protocol> self._protocol
        try:
            protocol._close(self)
        except (ssl.SSLError, exceptions.DatabaseError):
            pass

    def commit(self):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            Message message
        message = self._create_message(CommitMessage)
        protocol._process_single_message(message)

    def connect(self, ConnectParamsImpl params):
        # specify that binding a string to a LOB value is possible in thin
        # mode without the use of asyncio (will be removed in a future release)
        self._allow_bind_str_to_lob = True

        try:
            self._pre_connect(params)
            self._connect_with_params(params)
            self._post_connect_phase_two(params)
        except:
            self._force_close()
            raise

    def create_temp_lob_impl(self, DbType dbtype):
        cdef ThinLobImpl lob_impl = self._create_lob_impl(dbtype)
        lob_impl.create_temp()
        return lob_impl

    def get_type(self, object conn, str name):
        cdef ThinDbObjectTypeCache cache = \
                get_dbobject_type_cache(self._dbobject_type_cache_num)
        return cache.get_type(conn, name)

    def ping(self):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            Message message
        message = self._create_message(PingMessage)
        protocol._process_single_message(message)

    def rollback(self):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            Message message
        message = self._create_message(RollbackMessage)
        protocol._process_single_message(message)

    def set_call_timeout(self, uint32_t value):
        self._protocol._transport.set_timeout(value / 1000)
        self._call_timeout = value

    def tpc_begin(self, xid, uint32_t flags, uint32_t timeout):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            TransactionSwitchMessage message
        message = self._create_message(TransactionSwitchMessage)
        message.operation = TNS_TPC_TXN_START
        message.xid = xid
        message.flags = flags
        message.timeout = timeout
        protocol._process_single_message(message)
        self._transaction_context = message.context

    def tpc_commit(self, xid, bint one_phase):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_tpc_commit_message(xid, one_phase)
        protocol._process_single_message(message)
        self._check_tpc_commit_state(message.state, one_phase)

    def tpc_end(self, xid, uint32_t flags):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            TransactionSwitchMessage message
        message = self._create_message(TransactionSwitchMessage)
        message.operation = TNS_TPC_TXN_DETACH
        message.xid = xid
        message.context = self._transaction_context
        message.flags = flags
        protocol._process_single_message(message)
        self._transaction_context = None

    def tpc_prepare(self, xid):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_message(TransactionChangeStateMessage)
        message.operation = TNS_TPC_TXN_PREPARE
        message.xid = xid
        message.context = self._transaction_context
        protocol._process_single_message(message)
        if message.state == TNS_TPC_TXN_STATE_REQUIRES_COMMIT:
            return True
        elif message.state == TNS_TPC_TXN_STATE_READ_ONLY:
            return False
        errors._raise_err(errors.ERR_UNKNOWN_TRANSACTION_STATE,
                          state=message.state)

    def tpc_rollback(self, xid):
        cdef:
            Protocol protocol = <Protocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_tpc_rollback_message(xid)
        protocol._process_single_message(message)
        if message.state != TNS_TPC_TXN_STATE_ABORTED:
            errors._raise_err(errors.ERR_UNKNOWN_TRANSACTION_STATE,
                              state=message.state)
        self._transaction_context = None


cdef class AsyncThinConnImpl(BaseThinConnImpl):

    def __init__(self, str dsn, ConnectParamsImpl params):
        BaseThinConnImpl.__init__(self, dsn, params)
        self._protocol = AsyncProtocol()

    cdef BaseCursorImpl _create_cursor_impl(self):
        """
        Internal method for creating an empty cursor implementation object.
        """
        return AsyncThinCursorImpl.__new__(AsyncThinCursorImpl, self)

    async def _complete_pipeline_op(self, Message message):
        """
        Completes a particular pipeline operation.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            PipelineOpResultImpl result_impl = message.pipeline_result_impl
            MessageWithData fetch_message, message_with_data
            PipelineOpImpl op_impl = result_impl.operation
            uint8_t op_type = op_impl.op_type
            AsyncThinCursorImpl cursor_impl

        # all operations other than commit make use of a cursor
        if op_type == PIPELINE_OP_TYPE_COMMIT:
            return 0

        # keep warning, if applicable
        message_with_data = <MessageWithData> message
        result_impl.warning = message_with_data.warning

        # resend the message if that is required (for operations that fetch
        # LOBS, for example)
        cursor_impl = message_with_data.cursor_impl
        if message.resend:
            await protocol._process_message(message)
            if op_type in (
                PIPELINE_OP_TYPE_FETCH_ONE,
                PIPELINE_OP_TYPE_FETCH_MANY,
                PIPELINE_OP_TYPE_FETCH_ALL,
            ):
                while cursor_impl._buffer_rowcount > 0:
                    result_impl.rows.append(cursor_impl._create_row())
        result_impl.fetch_info_impls = cursor_impl.fetch_info_impls

        # for fetchall(), perform as many round trips as are required to
        # complete the fetch
        if op_type == PIPELINE_OP_TYPE_FETCH_ALL:
            fetch_message = cursor_impl._create_message(
                FetchMessage, message_with_data.cursor
            )
            while cursor_impl._more_rows_to_fetch:
                await protocol._process_single_message(fetch_message)
                while cursor_impl._buffer_rowcount > 0:
                    result_impl.rows.append(cursor_impl._create_row())
                if op_type != PIPELINE_OP_TYPE_FETCH_ALL:
                    break

        # for PL/SQL blocks that required a single execute, perform any
        # remaining executes now
        if op_type == PIPELINE_OP_TYPE_EXECUTE_MANY \
                and message_with_data.num_execs < op_impl.num_execs:
            while op_impl.num_execs > 0:
                op_impl.num_execs -= 1
                message_with_data.offset += 1
                if not cursor_impl._statement.requires_single_execute():
                    break
                await protocol._process_message(message)
            if op_impl.num_execs > 0:
                message_with_data.num_execs = op_impl.num_execs
                await protocol._process_message(message)

        # populate the metadata for any partial types observed during the
        # execution of the pipeline
        if message_with_data.type_cache is not None:
            conn = message_with_data.cursor.connection
            await message_with_data.type_cache.populate_partial_types(conn)

    async def _complete_pipeline_ops(
        self, list messages, bint continue_on_error
    ):
        """
        Completes any pipeline operations that have not actually completed.
        This could be due to the fact that LOBs were fetched or a fetch all
        operation has more rows to fetch.
        """
        cdef:
            PipelineOpResultImpl result_impl
            Message message
        for message in messages:
            result_impl = message.pipeline_result_impl
            if result_impl.error is not None:
                continue
            try:
                await self._complete_pipeline_op(message)
            except Exception as e:
                if not continue_on_error:
                    raise
                result_impl._capture_err(e)

    async def _connect_with_address(self, Address address,
                                    Description description,
                                    ConnectParamsImpl params,
                                    str connect_string,
                                    bint raise_exception):
        """
        Internal method used for connecting with the given description and
        address.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
        try:
            await protocol._connect_phase_one(self, params, description,
                                              address, connect_string)
        except (exceptions.DatabaseError, socket.gaierror,
                ConnectionRefusedError) as e:
            if raise_exception:
                errors._raise_err(errors.ERR_CONNECTION_FAILED, cause=e,
                                  connection_id=description.connection_id)
            return 0
        except Exception as e:
            errors._raise_err(errors.ERR_CONNECTION_FAILED, cause=e,
                              connection_id=description.connection_id)
        self._post_connect_phase_one(description, params)
        await self._protocol._connect_phase_two(self, description, params)

    async def _connect_with_description(self, Description description,
                                        ConnectParamsImpl params,
                                        bint final_desc):
        """
        Internal method used for connecting with the given description. Retry
        connecting to the socket if an attempt fails and retry_count is
        specified in the connect string.
        """
        cdef:
            uint32_t i, j, k, num_attempts, num_lists, num_addresses
            AddressList address_list
            bint raise_exc = False
            str connect_string
            Address address
        num_lists = len(description.active_children)
        num_attempts = description.retry_count + 1
        connect_string = _get_connect_data(description, self._connection_id, params)
        for i in range(num_attempts):
            for j, address_list in enumerate(description.active_children):
                num_addresses = len(address_list.active_children)
                for k, address in enumerate(address_list.active_children):
                    if final_desc:
                        raise_exc = i == num_attempts - 1 \
                                and j == num_lists - 1 \
                                and k == num_addresses - 1
                    await self._connect_with_address(address, description,
                                                     params, connect_string,
                                                     raise_exc)
                    if not self._protocol._in_connect:
                        return 0
            await asyncio.sleep(description.retry_delay)

    async def _connect_with_params(self, ConnectParamsImpl params):
        """
        Internal method used for connecting with the given parameters.
        """
        cdef:
            DescriptionList description_list = params.description_list
            ssize_t i, num_descriptions
            Description description
            bint final_desc
        description_list._set_active_children()
        num_descriptions = len(description_list.active_children)
        for i, description in enumerate(description_list.active_children):
            final_desc = (i == num_descriptions - 1)
            await self._connect_with_description(description, params,
                                                 final_desc)
            if not self._protocol._in_connect:
                break

    cdef Message _create_message_for_pipeline_op(
        self, object conn, PipelineOpImpl op_impl
    ):
        """
        Creates a single message for a pipeline operation.
        """
        cdef:
            AsyncThinCursorImpl cursor_impl
            MessageWithData message
            uint32_t num_execs = 1
            object cursor
        if op_impl.op_type == PIPELINE_OP_TYPE_COMMIT:
            return self._create_message(CommitMessage)
        cursor = conn.cursor()
        cursor_impl = <AsyncThinCursorImpl> cursor._impl
        if op_impl.op_type == PIPELINE_OP_TYPE_CALL_FUNC:
            execute_args = cursor._call_get_execute_args(
                op_impl.name,
                op_impl.parameters,
                op_impl.keyword_parameters,
                cursor.var(op_impl.return_type)
            )
            cursor._prepare_for_execute(*execute_args)
        elif op_impl.op_type == PIPELINE_OP_TYPE_CALL_PROC:
            execute_args = cursor._call_get_execute_args(
                op_impl.name,
                op_impl.parameters,
                op_impl.keyword_parameters
            )
            cursor._prepare_for_execute(*execute_args)
        elif op_impl.op_type == PIPELINE_OP_TYPE_EXECUTE:
            cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
        elif op_impl.op_type == PIPELINE_OP_TYPE_EXECUTE_MANY:
            num_execs = cursor_impl._prepare_for_executemany(
                cursor, op_impl.statement, op_impl.parameters
            )
            op_impl.num_execs = num_execs
            if cursor_impl._statement.requires_single_execute():
                num_execs = 1
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_ONE:
            cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
            cursor_impl.prefetchrows = 1
            cursor_impl.arraysize = 1
            cursor_impl.rowfactory = op_impl.rowfactory
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_MANY:
            cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
            cursor_impl.prefetchrows = op_impl.num_rows
            cursor_impl.arraysize = op_impl.num_rows
            cursor_impl.rowfactory = op_impl.rowfactory
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_ALL:
            cursor._prepare_for_execute(op_impl.statement, op_impl.parameters)
            cursor_impl.prefetchrows = op_impl.arraysize
            cursor_impl.arraysize = op_impl.arraysize
            cursor_impl.rowfactory = op_impl.rowfactory
        else:
            errors._raise_err(errors.ERR_UNSUPPORTED_PIPELINE_OPERATION,
                              op_type=op_impl.op_type)
        cursor_impl._preprocess_execute(conn)
        message = cursor_impl._create_message(ExecuteMessage, cursor)
        message.num_execs = num_execs
        return message

    cdef list _create_messages_for_pipeline(
        self, object conn, list results, bint continue_on_error
    ):
        """
        Creates a list of messages for the pipeline and returns them after they
        have been submitted to the database for processing.
        """
        cdef:
            PipelineOpResultImpl result_impl
            PipelineOpImpl op_impl
            uint64_t token_num
            Message message
            object result
            list messages
        messages = []
        token_num = 1
        for result in results:
            result_impl = result._impl
            op_impl = result_impl.operation
            try:
                message = self._create_message_for_pipeline_op(conn, op_impl)
            except Exception as e:
                if not continue_on_error:
                    raise
                result_impl._capture_err(e)
                continue
            message.pipeline_result_impl = result_impl
            message.token_num = token_num
            token_num += 1
            messages.append(message)
        return messages

    cdef int _populate_pipeline_op_result(self, Message message) except -1:
        """
        Populates the pipeline operation result object.
        """
        cdef:
            MessageWithData message_with_data
            AsyncThinCursorImpl cursor_impl
            PipelineOpResultImpl result_impl
            PipelineOpImpl op_impl
            BindVar bind_var
        result_impl = message.pipeline_result_impl
        op_impl = result_impl.operation
        if op_impl.op_type == PIPELINE_OP_TYPE_COMMIT:
            return 0
        message_with_data = <MessageWithData> message
        cursor_impl = <AsyncThinCursorImpl> message_with_data.cursor_impl
        if op_impl.op_type == PIPELINE_OP_TYPE_CALL_FUNC:
            bind_var = <BindVar> cursor_impl.bind_vars[0]
            result_impl.return_value = bind_var.var_impl.get_value(0)
        elif op_impl.op_type in (
            PIPELINE_OP_TYPE_FETCH_ONE,
            PIPELINE_OP_TYPE_FETCH_MANY,
            PIPELINE_OP_TYPE_FETCH_ALL,
        ):
            result_impl.rows = []
            while cursor_impl._buffer_rowcount > 0:
                result_impl.rows.append(cursor_impl._create_row())

    cdef int _populate_pipeline_op_results(
        self, list messages, bint continue_on_error
    ) except -1:
        """
        Populates the pipeline operation result objects associated with the
        messages that were processed on the database.
        """
        cdef:
            PipelineOpResultImpl result_impl
            Message message
        for message in messages:
            result_impl = message.pipeline_result_impl
            if result_impl.error is not None:
                continue
            try:
                self._populate_pipeline_op_result(message)
            except Exception as e:
                if not continue_on_error:
                    raise
                result_impl._capture_err(e)

    async def _run_pipeline_op_without_pipelining(
        self, object conn, PipelineOpResultImpl result_impl
    ):
        """
        Runs a pipeline operation without the use of pipelining.
        """
        cdef:
            PipelineOpImpl op_impl = result_impl.operation
            object cursor
        if op_impl.op_type == PIPELINE_OP_TYPE_COMMIT:
            await conn.commit()
            return
        cursor = conn.cursor()
        if op_impl.op_type == PIPELINE_OP_TYPE_CALL_FUNC:
            result_impl.return_value = await cursor.callfunc(
                op_impl.name,
                op_impl.return_type,
                op_impl.parameters,
                op_impl.keyword_parameters,
            )
        elif op_impl.op_type == PIPELINE_OP_TYPE_CALL_PROC:
            await cursor.callproc(
                op_impl.name, op_impl.parameters, op_impl.keyword_parameters
            )
        elif op_impl.op_type == PIPELINE_OP_TYPE_EXECUTE:
            await cursor.execute(op_impl.statement, op_impl.parameters)
        elif op_impl.op_type == PIPELINE_OP_TYPE_EXECUTE_MANY:
            await cursor.executemany(op_impl.statement, op_impl.parameters)
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_ALL:
            await cursor.execute(op_impl.statement, op_impl.parameters)
            cursor.rowfactory = op_impl.rowfactory
            result_impl.rows = await cursor.fetchall()
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_MANY:
            await cursor.execute(op_impl.statement, op_impl.parameters)
            cursor.rowfactory = op_impl.rowfactory
            result_impl.rows = await cursor.fetchmany(op_impl.num_rows)
        elif op_impl.op_type == PIPELINE_OP_TYPE_FETCH_ONE:
            await cursor.execute(op_impl.statement, op_impl.parameters)
            cursor.rowfactory = op_impl.rowfactory
            result_impl.rows = await cursor.fetchmany(1)
        else:
            errors._raise_err(errors.ERR_UNSUPPORTED_PIPELINE_OPERATION,
                              op_type=op_impl.op_type)
        result_impl.warning = cursor.warning
        result_impl.fetch_info_impls = cursor._impl.fetch_info_impls

    cdef int _send_messages_for_pipeline(
        self, list messages, bint continue_on_error
    ) except -1:
        """
        Sends the messages for the pipeline to the database for processing.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            Message message
        for message in messages:
            try:
                message.send(protocol._write_buf)
            except Exception as e:
                if not continue_on_error:
                    raise
                message.pipeline_result_impl._capture_err(e)

    async def change_password(self, str old_password, str new_password):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            ChangePasswordMessage message
        message = self._create_message(ChangePasswordMessage)
        message.password = old_password.encode()
        message.newpassword = new_password.encode()
        await protocol._process_single_message(message)

    async def close(self, bint in_del=False):
        """
        Sends the messages needed to disconnect from the database.
        """
        cdef BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
        try:
            await protocol._close(self)
        except (ssl.SSLError, exceptions.DatabaseError):
            pass

    async def commit(self):
        """
        Sends the message to commit any pending transaction.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            Message message
        message = self._create_message(CommitMessage)
        await protocol._process_single_message(message)

    async def connect(self, ConnectParamsImpl params):
        """
        Sends the messages needed to connect to the database.
        """
        cdef BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
        protocol._read_buf._loop = asyncio.get_running_loop()
        try:
            self._pre_connect(params)
            await self._connect_with_params(params)
            self._post_connect_phase_two(params)
        except:
            self._force_close()
            raise

    async def create_temp_lob_impl(self, DbType dbtype):
        cdef AsyncThinLobImpl lob_impl = self._create_lob_impl(dbtype)
        await lob_impl.create_temp()
        return lob_impl

    async def get_type(self, object conn, str name):
        cdef AsyncThinDbObjectTypeCache cache = \
                get_dbobject_type_cache(self._dbobject_type_cache_num)
        return await cache.get_type(conn, name)

    async def ping(self):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            Message message
        message = self._create_message(PingMessage)
        await protocol._process_single_message(message)

    async def rollback(self):
        """
        Sends the message to roll back any pending transaction.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            Message message
        message = self._create_message(RollbackMessage)
        await protocol._process_single_message(message)

    async def run_pipeline_with_pipelining(
        self, object conn, list results, bint continue_on_error
    ):
        """
        Run the pipeline with pipelining when the database supports it.
        """
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            list messages
        messages = self._create_messages_for_pipeline(
            conn, results, continue_on_error
        )
        if messages:
            protocol._read_buf.reset_packets()
            if continue_on_error:
                self.pipeline_mode = TNS_PIPELINE_MODE_CONTINUE_ON_ERROR
            else:
                self.pipeline_mode = TNS_PIPELINE_MODE_ABORT_ON_ERROR
            self._send_messages_for_pipeline(messages, continue_on_error)
            await protocol.end_pipeline(self, messages, continue_on_error)
            self._populate_pipeline_op_results(messages, continue_on_error)
            await self._complete_pipeline_ops(messages, continue_on_error)

    async def run_pipeline_without_pipelining(
        self, object conn, list results, bint continue_on_error
    ):
        """
        Run the pipeline without pipelining when the database doesn't support
        pipelining. Call timeouts are disabled for consistency with when
        run with pipelining.
        """
        cdef:
            uint32_t call_timeout = self._call_timeout
            PipelineOpResultImpl result_impl
            object result
        try:
            for result in results:
                result_impl = result._impl
                try:
                    await self._run_pipeline_op_without_pipelining(
                        conn, result_impl
                    )
                except Exception as e:
                    if not continue_on_error:
                        raise
                    result_impl._capture_err(e)
        finally:
            self._call_timeout = call_timeout

    def set_call_timeout(self, uint32_t value):
        self._call_timeout = value

    def supports_pipelining(self):
        """
        Returns whether the connection supports pipelining. Currently this is
        only supported with asyncio and Oracle Database 23ai and later.
        """
        return self._protocol._caps.supports_pipelining

    async def tpc_begin(self, xid, uint32_t flags, uint32_t timeout):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            TransactionSwitchMessage message
        message = self._create_message(TransactionSwitchMessage)
        message.operation = TNS_TPC_TXN_START
        message.xid = xid
        message.flags = flags
        message.timeout = timeout
        await protocol._process_single_message(message)
        self._transaction_context = message.context

    async def tpc_commit(self, xid, bint one_phase):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_tpc_commit_message(xid, one_phase)
        await protocol._process_single_message(message)
        self._check_tpc_commit_state(message.state, one_phase)

    async def tpc_end(self, xid, uint32_t flags):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            TransactionSwitchMessage message
        message = self._create_message(TransactionSwitchMessage)
        message.operation = TNS_TPC_TXN_DETACH
        message.xid = xid
        message.context = self._transaction_context
        message.flags = flags
        await protocol._process_single_message(message)
        self._transaction_context = None

    async def tpc_prepare(self, xid):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_message(TransactionChangeStateMessage)
        message.operation = TNS_TPC_TXN_PREPARE
        message.xid = xid
        message.context = self._transaction_context
        await protocol._process_single_message(message)
        if message.state == TNS_TPC_TXN_STATE_REQUIRES_COMMIT:
            return True
        elif message.state == TNS_TPC_TXN_STATE_READ_ONLY:
            return False
        errors._raise_err(errors.ERR_UNKNOWN_TRANSACTION_STATE,
                          state=message.state)

    async def tpc_rollback(self, xid):
        cdef:
            BaseAsyncProtocol protocol = <BaseAsyncProtocol> self._protocol
            TransactionChangeStateMessage message
        message = self._create_tpc_rollback_message(xid)
        await protocol._process_single_message(message)
        if message.state != TNS_TPC_TXN_STATE_ABORTED:
            errors._raise_err(errors.ERR_UNKNOWN_TRANSACTION_STATE,
                              state=message.state)
        self._transaction_context = None
