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
# Cython file defining the thick Connection implementation class (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------

ctypedef int (*dpiConnSetTextAttrFunc)(dpiConn*, const char*, uint32_t) nogil

cdef class ConnectionParams:
    cdef:
        bytes dsn
        bytes username
        bytes password
        bytes cclass
        bytes new_password
        bytes edition
        bytes tag
        bytes token
        bytes private_key
        bytes driver_name

        const char *dsn_ptr
        const char *username_ptr
        const char *password_ptr
        const char *cclass_ptr
        const char *new_password_ptr
        const char *edition_ptr
        const char *tag_ptr
        const char *token_ptr
        const char *private_key_ptr
        const char *driver_name_ptr

        uint32_t dsn_len
        uint32_t username_len
        uint32_t password_len
        uint32_t cclass_len
        uint32_t new_password_len
        uint32_t edition_len
        uint32_t tag_len
        uint32_t token_len
        uint32_t private_key_len
        uint32_t driver_name_len

        uint32_t num_app_context
        list bytes_references
        dpiAppContext *app_context

        uint32_t num_sharding_key_columns
        dpiShardingKeyColumn *sharding_key_columns
        uint32_t num_super_sharding_key_columns
        dpiShardingKeyColumn *super_sharding_key_columns

    def __dealloc__(self):
        if self.app_context is not NULL:
            cpython.PyMem_Free(self.app_context)
        if self.sharding_key_columns is not NULL:
            cpython.PyMem_Free(self.sharding_key_columns)
        if self.super_sharding_key_columns is not NULL:
            cpython.PyMem_Free(self.super_sharding_key_columns)

    cdef int _process_context_str(self, str value, const char **ptr,
                                  uint32_t *length) except -1:
        cdef bytes temp
        temp = value.encode()
        self.bytes_references.append(temp)
        ptr[0] = temp
        length[0] = <uint32_t> len(temp)

    cdef int _process_sharding_value(self, object value,
                                     dpiShardingKeyColumn *column) except -1:
        """
        Process a sharding column value and place it in the format required by
        ODPI-C.
        """
        cdef:
            dpiTimestamp* timestamp
            bytes temp
        if isinstance(value, str):
            temp = value.encode()
            self.bytes_references.append(temp)
            column.oracleTypeNum = DPI_ORACLE_TYPE_VARCHAR
            column.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            column.value.asBytes.ptr = temp
            column.value.asBytes.length = <uint32_t> len(temp)
        elif isinstance(value, (int, float, PY_TYPE_DECIMAL)):
            temp = str(value).encode()
            self.bytes_references.append(temp)
            column.oracleTypeNum = DPI_ORACLE_TYPE_NUMBER
            column.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            column.value.asBytes.ptr = temp
            column.value.asBytes.length = <uint32_t> len(temp)
        elif isinstance(value, bytes):
            self.bytes_references.append(value)
            column.oracleTypeNum = DPI_ORACLE_TYPE_RAW
            column.nativeTypeNum = DPI_NATIVE_TYPE_BYTES
            column.value.asBytes.ptr = <bytes> value
            column.value.asBytes.length = <uint32_t> len(value)
        elif isinstance(value, PY_TYPE_DATETIME):
            column.oracleTypeNum = DPI_ORACLE_TYPE_DATE
            column.nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP
            timestamp = &column.value.asTimestamp
            memset(timestamp, 0, sizeof(dpiTimestamp))
            timestamp.year = cydatetime.datetime_year(value)
            timestamp.month = cydatetime.datetime_month(value)
            timestamp.day = cydatetime.datetime_day(value)
            timestamp.hour = cydatetime.datetime_hour(value)
            timestamp.minute = cydatetime.datetime_minute(value)
            timestamp.second = cydatetime.datetime_second(value)
            timestamp.fsecond = cydatetime.datetime_microsecond(value) * 1000
        elif isinstance(value, PY_TYPE_DATE):
            column.oracleTypeNum = DPI_ORACLE_TYPE_DATE
            column.nativeTypeNum = DPI_NATIVE_TYPE_TIMESTAMP
            timestamp = &column.value.asTimestamp
            memset(timestamp, 0, sizeof(dpiTimestamp))
            timestamp.year = cydatetime.date_year(value)
            timestamp.month = cydatetime.date_month(value)
            timestamp.day = cydatetime.date_day(value)
        else:
            errors._raise_err(errors.ERR_PYTHON_VALUE_NOT_SUPPORTED,
                              type_name=type(value).__name__)

    cdef process_appcontext(self, list entries):
        cdef:
            object namespace, name, value
            dpiAppContext *entry
            ssize_t num_bytes
            bytes temp
            uint32_t i
        if self.bytes_references is None:
            self.bytes_references = []
        self.num_app_context = <uint32_t> len(entries)
        num_bytes = self.num_app_context * sizeof(dpiAppContext)
        self.app_context = <dpiAppContext*> cpython.PyMem_Malloc(num_bytes)
        for i in range(self.num_app_context):
            namespace, name, value = entries[i]
            entry = &self.app_context[i]
            self._process_context_str(namespace, &entry.namespaceName,
                                      &entry.namespaceNameLength)
            self._process_context_str(name, &entry.name, &entry.nameLength)
            self._process_context_str(value, &entry.value, &entry.valueLength)

    cdef int process_sharding_key(self, list entries, bint is_super) except -1:
        """
        Process the (super) sharding key and place it in the format required by
        ODPI-C.
        """
        cdef:
            dpiShardingKeyColumn *columns
            uint32_t num_columns
            ssize_t num_bytes, i
        if self.bytes_references is None:
            self.bytes_references = []
        num_columns = <uint32_t> len(entries)
        num_bytes = num_columns * sizeof(dpiShardingKeyColumn)
        columns = <dpiShardingKeyColumn*> cpython.PyMem_Malloc(num_bytes)
        if is_super:
            self.super_sharding_key_columns = columns
            self.num_super_sharding_key_columns = num_columns
        else:
            self.sharding_key_columns = columns
            self.num_sharding_key_columns = num_columns
        for i, entry in enumerate(entries):
            self._process_sharding_value(entry, &columns[i])


@cython.freelist(8)
cdef class ThickXid:
    cdef:
        StringBuffer global_transaction_id_buf
        StringBuffer branch_qualifier_buf
        dpiXid* xid_ptr
        dpiXid xid_buf

    def __init__(self, xid):
        if xid is not None:
            self.global_transaction_id_buf = StringBuffer()
            self.global_transaction_id_buf.set_value(xid.global_transaction_id)
            self.branch_qualifier_buf = StringBuffer()
            self.branch_qualifier_buf.set_value(xid.branch_qualifier)
            self.xid_buf.formatId = xid.format_id
            self.xid_buf.globalTransactionId = \
                    self.global_transaction_id_buf.ptr
            self.xid_buf.globalTransactionIdLength = \
                    self.global_transaction_id_buf.length
            self.xid_buf.branchQualifier = \
                    self.branch_qualifier_buf.ptr
            self.xid_buf.branchQualifierLength = \
                    self.branch_qualifier_buf.length
            self.xid_ptr = &self.xid_buf


cdef class ThickConnImpl(BaseConnImpl):
    cdef:
        dpiConn *_handle
        bint _is_external
        public str tag

    def __dealloc__(self):
        if self._handle != NULL:
            dpiConn_release(self._handle)

    cdef BaseCursorImpl _create_cursor_impl(self):
        """
        Internal method for creating an empty cursor implementation object.
        """
        return ThickCursorImpl.__new__(ThickCursorImpl, self)

    def _get_oci_attr(self, uint32_t handle_type, uint32_t attr_num,
                      uint32_t attr_type):
        """
        Internal method for getting the value of an OCI attribute on the
        connection.
        """
        cdef:
            dpiDataBuffer value
            uint32_t value_len
        if dpiConn_getOciAttr(self._handle, handle_type, attr_num, &value,
                              &value_len) < 0:
            _raise_from_odpi()
        return _convert_oci_attr_to_python(attr_type, &value, value_len)

    def _set_oci_attr(self, uint32_t handle_type, uint32_t attr_num,
                      uint32_t attr_type, object value):
        """
        Internal method for setting the value of an OCI attribute on the
        connection.
        """
        cdef:
            StringBuffer str_buf = StringBuffer()
            void *oci_value = NULL
            dpiDataBuffer oci_buf
            uint32_t oci_len = 0
        _convert_python_to_oci_attr(value, attr_type, str_buf, &oci_buf,
                                    &oci_value, &oci_len)
        if dpiConn_setOciAttr(self._handle, handle_type, attr_num, oci_value,
                              oci_len) < 0:
            _raise_from_odpi()

    cdef int _set_text_attr(self, dpiConnSetTextAttrFunc func,
                            str value) except -1:
        cdef:
            uint32_t value_length
            const char *value_ptr
            bytes value_bytes
        if value is not None:
            value_bytes = value.encode()
            value_ptr = value_bytes
            value_length = len(value_bytes)
        else:
            value_ptr = NULL
            value_length = 0
        if func(self._handle, value_ptr, value_length) < 0:
            _raise_from_odpi()

    def cancel(self):
        cdef int status
        with nogil:
            status = dpiConn_breakExecution(self._handle)
        if status < 0:
            _raise_from_odpi()

    def change_password(self, str old_password, str new_password):
        cdef:
            bytes username_bytes, old_password_bytes, new_password_bytes
            uint32_t username_len = 0, old_password_len = 0
            const char *old_password_ptr = NULL
            const char *new_password_ptr = NULL
            const char *username_ptr = NULL
            uint32_t new_password_len = 0
            int status
        if self.username is not None:
            username_bytes = self.username.encode()
            username_ptr = username_bytes
            username_len = <uint32_t> len(username_bytes)
        old_password_bytes = old_password.encode()
        old_password_ptr = old_password_bytes
        old_password_len = <uint32_t> len(old_password_bytes)
        new_password_bytes = new_password.encode()
        new_password_ptr = new_password_bytes
        new_password_len = <uint32_t> len(new_password_bytes)
        with nogil:
            status = dpiConn_changePassword(self._handle, username_ptr,
                                            username_len, old_password_ptr,
                                            old_password_len, new_password_ptr,
                                            new_password_len)
        if status < 0:
            _raise_from_odpi()

    def get_is_healthy(self):
        cdef bint is_healthy
        if dpiConn_getIsHealthy(self._handle, &is_healthy) < 0:
            _raise_from_odpi()
        return is_healthy

    def close(self, bint in_del=False):
        cdef:
            uint32_t mode = DPI_MODE_CONN_CLOSE_DEFAULT
            const char *tag_ptr = NULL
            uint32_t tag_length = 0
            bytes tag_bytes
            int status
        if in_del and self._is_external:
            return 0
        if self.tag is not None:
            mode = DPI_MODE_CONN_CLOSE_RETAG
            tag_bytes = self.tag.encode()
            tag_ptr = tag_bytes
            tag_length = <uint32_t> len(tag_bytes)
        with nogil:
            status = dpiConn_close(self._handle, mode, tag_ptr, tag_length)
            if status == DPI_SUCCESS:
                dpiConn_release(self._handle)
                self._handle = NULL
        if status < 0 and not in_del:
            _raise_from_odpi()

    def commit(self):
        cdef int status
        with nogil:
            status = dpiConn_commit(self._handle)
        if status < 0:
            _raise_from_odpi()

    def connect(self, ConnectParamsImpl user_params, ThickPoolImpl pool_impl):
        cdef:
            str full_user, cclass, token, private_key
            bytes password_bytes, new_password_bytes
            dpiCommonCreateParams common_params
            dpiConnCreateParams conn_params
            ConnectParamsImpl pool_params
            dpiAccessToken access_token
            dpiVersionInfo version_info
            dpiErrorInfo error_info
            ConnectionParams params
            int status

        # specify that binding a string to a LOB value is possible in thick
        # mode (will be removed in a future release)
        self._allow_bind_str_to_lob = True

        # if the connection is part of the pool, get the pool creation params
        if pool_impl is not None:
            pool_params = pool_impl.connect_params
            self.username = pool_impl.username
            self.dsn = pool_impl.dsn

        # set up connection parameters
        params = ConnectionParams()
        password_bytes = user_params._get_password()
        new_password_bytes = user_params._get_new_password()
        full_user = user_params.get_full_user()
        if full_user is not None:
            params.username = full_user.encode()
            params.username_ptr = params.username
            params.username_len = <uint32_t> len(params.username)
        if password_bytes is not None:
            params.password = password_bytes
            params.password_ptr = params.password
            params.password_len = <uint32_t> len(params.password)
        if self.dsn is not None:
            params.dsn = self.dsn.encode()
            params.dsn_ptr = params.dsn
            params.dsn_len = <uint32_t> len(params.dsn)
        if pool_impl is None \
                or user_params._default_description.cclass is not None:
            cclass = user_params._default_description.cclass
        else:
            cclass = pool_params._default_description.cclass
        if cclass is not None:
            params.cclass = cclass.encode()
            params.cclass_ptr = params.cclass
            params.cclass_len = <uint32_t> len(params.cclass)
        if new_password_bytes is not None:
            params.new_password = new_password_bytes
            params.new_password_ptr = params.new_password
            params.new_password_len = <uint32_t> len(params.new_password)
        if user_params.edition is not None:
            params.edition = user_params.edition.encode()
            params.edition_ptr = params.edition
            params.edition_len = <uint32_t> len(params.edition)
        if user_params.tag is not None:
            params.tag = user_params.tag.encode()
            params.tag_ptr = params.tag
            params.tag_len = <uint32_t> len(params.tag)
        if user_params.appcontext:
            params.process_appcontext(user_params.appcontext)
        if user_params.shardingkey:
            params.process_sharding_key(user_params.shardingkey, False)
        if user_params.supershardingkey:
            params.process_sharding_key(user_params.supershardingkey, True)
        if user_params._token is not None \
                or user_params.access_token_callback is not None:
            token = user_params._get_token()
            private_key = user_params._get_private_key()
            params.token = token.encode()
            params.token_ptr = params.token
            params.token_len = <uint32_t> len(params.token)
            if private_key is not None:
                params.private_key = private_key.encode()
                params.private_key_ptr = params.private_key
                params.private_key_len = <uint32_t> len(params.private_key)
        if user_params.driver_name is not None:
            params.driver_name = user_params.driver_name.encode()[:30]
            params.driver_name_ptr = params.driver_name
            params.driver_name_len = <uint32_t> len(params.driver_name)

        # set up common creation parameters
        if dpiContext_initCommonCreateParams(driver_info.context,
                                             &common_params) < 0:
            _raise_from_odpi()
        common_params.createMode |= DPI_MODE_CREATE_THREADED
        if user_params.events:
            common_params.createMode |= DPI_MODE_CREATE_EVENTS
        if user_params.edition is not None:
            common_params.edition = params.edition_ptr
            common_params.editionLength = params.edition_len
        if params.token is not None:
            access_token.token = params.token_ptr
            access_token.tokenLength = params.token_len
            access_token.privateKey = params.private_key_ptr
            access_token.privateKeyLength = params.private_key_len
            common_params.accessToken = &access_token
        if user_params.driver_name is not None:
            common_params.driverName = params.driver_name_ptr
            common_params.driverNameLength = params.driver_name_len

        # set up connection specific creation parameters
        if dpiContext_initConnCreateParams(driver_info.context,
                                           &conn_params) < 0:
            _raise_from_odpi()
        if params.username_len == 0 and params.password_len == 0:
            conn_params.externalAuth = 1
        else:
            conn_params.externalAuth = user_params.externalauth
        if params.cclass is not None:
            conn_params.connectionClass = params.cclass_ptr
            conn_params.connectionClassLength = params.cclass_len
        if new_password_bytes is not None:
            conn_params.newPassword = params.new_password_ptr
            conn_params.newPasswordLength = params.new_password_len
        if user_params.appcontext:
            conn_params.appContext = params.app_context
            conn_params.numAppContext = params.num_app_context
        if user_params.shardingkey:
            conn_params.shardingKeyColumns = params.sharding_key_columns
            conn_params.numShardingKeyColumns = params.num_sharding_key_columns
        if user_params.supershardingkey:
            conn_params.superShardingKeyColumns = \
                    params.super_sharding_key_columns
            conn_params.numSuperShardingKeyColumns = \
                    params.num_super_sharding_key_columns
        if user_params.tag is not None:
            conn_params.tag = params.tag_ptr
            conn_params.tagLength = params.tag_len
        if user_params._external_handle != 0:
            conn_params.externalHandle = <void*> user_params._external_handle
            self._is_external = True
        if pool_impl is not None:
            conn_params.pool = pool_impl._handle
        common_params.stmtCacheSize = user_params.stmtcachesize
        conn_params.authMode = user_params.mode
        conn_params.matchAnyTag = user_params.matchanytag
        if user_params._default_description.purity != PURITY_DEFAULT:
            conn_params.purity = user_params._default_description.purity
        elif pool_impl is not None:
            conn_params.purity = pool_params._default_description.purity

        # perform connection
        with nogil:
            status = dpiConn_create(driver_info.context, params.username_ptr,
                                    params.username_len, params.password_ptr,
                                    params.password_len, params.dsn_ptr,
                                    params.dsn_len, &common_params,
                                    &conn_params, &self._handle)
            dpiContext_getError(driver_info.context, &error_info)
        if status < 0:
            _raise_from_info(&error_info)
        elif error_info.isWarning:
            self.warning = _create_new_from_info(&error_info)
        if conn_params.outNewSession and pool_impl is not None \
                and self.warning is None:
            self.warning = pool_impl.warning
        if dpiConn_getServerVersion(self._handle, NULL, NULL,
                                    &version_info) < 0:
            _raise_from_odpi()
        self.server_version = (
            version_info.versionNum,
            version_info.releaseNum,
            version_info.updateNum,
            version_info.portReleaseNum,
            version_info.portUpdateNum
        )
        if driver_info.client_version_info.versionNum >= 23 \
                and version_info.versionNum >= 23:
            self.supports_bool = True
            self._oson_max_fname_size = 65535

        # determine if session callback should be invoked; this takes place if
        # the connection is newly created by the pool or if the requested tag
        # does not match the actual tag
        if (conn_params.outNewSession \
                or conn_params.outTagLength != params.tag_len \
                or (params.tag_len > 0 \
                and conn_params.outTag[:conn_params.outTagLength] !=  \
                params.tag_ptr[:conn_params.outTagLength])):
            self.invoke_session_callback = True

        # set tag property, if applicable
        if conn_params.outTagLength > 0:
            self.tag = conn_params.outTag[:conn_params.outTagLength].decode()

    def create_msg_props_impl(self):
        cdef ThickMsgPropsImpl impl
        impl = ThickMsgPropsImpl.__new__(ThickMsgPropsImpl)
        impl._conn_impl = self
        if dpiConn_newMsgProps(self._handle, &impl._handle) < 0:
            _raise_from_odpi()
        return impl

    def create_queue_impl(self):
        return ThickQueueImpl.__new__(ThickQueueImpl)

    def create_soda_database_impl(self, conn):
        cdef ThickSodaDbImpl impl = ThickSodaDbImpl.__new__(ThickSodaDbImpl)
        impl.supports_json = driver_info.soda_use_json_desc
        impl._conn = conn
        if dpiConn_getSodaDb(self._handle, &impl._handle) < 0:
            _raise_from_odpi()
        return impl

    def create_subscr_impl(self, object conn, object callback,
                           uint32_t namespace, str name, uint32_t protocol,
                           str ip_address, uint32_t port, uint32_t timeout,
                           uint32_t operations, uint32_t qos,
                           uint8_t grouping_class, uint32_t grouping_value,
                           uint8_t grouping_type, bint client_initiated):
        cdef ThickSubscrImpl impl = ThickSubscrImpl.__new__(ThickSubscrImpl)
        impl.connection = conn
        impl.callback = callback
        impl.namespace = namespace
        impl.name = name
        impl.protocol = protocol
        impl.ip_address = ip_address
        impl.port = port
        impl.timeout = timeout
        impl.operations = operations
        impl.qos = qos
        impl.grouping_class = grouping_class
        impl.grouping_value = grouping_value
        impl.grouping_type = grouping_type
        impl.client_initiated = client_initiated
        return impl

    def create_temp_lob_impl(self, DbType dbtype):
        return ThickLobImpl._create(self, dbtype, NULL)

    def get_call_timeout(self):
        cdef uint32_t value
        if dpiConn_getCallTimeout(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_current_schema(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getCurrentSchema(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_db_domain(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getDbDomain(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_db_name(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getDbName(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_edition(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getEdition(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_external_name(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getExternalName(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_handle(self):
        cdef void* handle
        if dpiConn_getHandle(self._handle, &handle) < 0:
            _raise_from_odpi()
        return <uint64_t> handle

    def get_instance_name(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getInstanceName(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_internal_name(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getInternalName(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_ltxid(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getLTXID(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        return value[:value_length]

    def get_max_identifier_length(self):
        cdef dpiConnInfo info
        if dpiConn_getInfo(self._handle, &info) < 0:
            _raise_from_odpi()
        if info.maxIdentifierLength != 0:
            return info.maxIdentifierLength

    def get_max_open_cursors(self):
        cdef uint32_t value
        if dpiConn_getMaxOpenCursors(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_service_name(self):
        cdef:
            uint32_t value_length
            const char *value
        if dpiConn_getServiceName(self._handle, &value, &value_length) < 0:
            _raise_from_odpi()
        if value is not NULL:
            return value[:value_length].decode()

    def get_stmt_cache_size(self):
        cdef uint32_t value
        if dpiConn_getStmtCacheSize(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_transaction_in_progress(self):
        cdef bint value
        if dpiConn_getTransactionInProgress(self._handle, &value) < 0:
            _raise_from_odpi()
        return value

    def get_type(self, object conn, str name):
        cdef:
            dpiObjectType *handle
            const char *name_ptr
            uint32_t name_len
            bytes name_bytes
            int status
        name_bytes = name.encode()
        name_ptr = name_bytes
        name_len = <uint32_t> len(name_bytes)
        with nogil:
            status = dpiConn_getObjectType(self._handle, name_ptr, name_len,
                                           &handle)
        if status < 0:
            _raise_from_odpi()
        try:
            return ThickDbObjectTypeImpl._from_handle(self, handle)
        finally:
            dpiObjectType_release(handle)

    def set_action(self, str value):
        self._set_text_attr(dpiConn_setAction, value)

    def set_call_timeout(self, uint32_t value):
        if dpiConn_setCallTimeout(self._handle, value) < 0:
            _raise_from_odpi()

    def set_client_identifier(self, str value):
        self._set_text_attr(dpiConn_setClientIdentifier, value)

    def set_client_info(self, str value):
        self._set_text_attr(dpiConn_setClientInfo, value)

    def set_current_schema(self, str value):
        self._set_text_attr(dpiConn_setCurrentSchema, value)

    def set_dbop(self, str value):
        self._set_text_attr(dpiConn_setDbOp, value)

    def ping(self):
        cdef int status
        with nogil:
            status = dpiConn_ping(self._handle)
        if status < 0:
            _raise_from_odpi()

    def rollback(self):
        cdef int status
        with nogil:
            status = dpiConn_rollback(self._handle)
        if status < 0:
            _raise_from_odpi()

    def set_econtext_id(self, value):
        self._set_text_attr(dpiConn_setEcontextId, value)

    def set_external_name(self, str value):
        self._set_text_attr(dpiConn_setExternalName, value)

    def set_internal_name(self, str value):
        self._set_text_attr(dpiConn_setInternalName, value)

    def set_module(self, str value):
        self._set_text_attr(dpiConn_setModule, value)

    def set_stmt_cache_size(self, uint32_t value):
        if dpiConn_setStmtCacheSize(self._handle, value) < 0:
            _raise_from_odpi()

    def shutdown(self, uint32_t mode):
        cdef int status
        with nogil:
            status = dpiConn_shutdownDatabase(self._handle, mode)
        if status < 0:
            _raise_from_odpi()

    def startup(self, bint force, bint restrict, str pfile):
        cdef:
            uint32_t mode, pfile_length = 0
            const char *pfile_ptr = NULL
            bytes temp
            int status

        mode = DPI_MODE_STARTUP_DEFAULT
        if force:
            mode |= DPI_MODE_STARTUP_FORCE
        if restrict:
            mode |= DPI_MODE_STARTUP_RESTRICT
        if pfile is not None:
            temp = pfile.encode()
            pfile_ptr = temp
            pfile_length = len(pfile_ptr)
        with nogil:
            status = dpiConn_startupDatabaseWithPfile(self._handle, pfile_ptr,
                                                      pfile_length, mode)
        if status < 0:
            _raise_from_odpi()

    def tpc_begin(self, xid, uint32_t flags, uint32_t timeout):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            int status
        with nogil:
            status = dpiConn_tpcBegin(self._handle, thick_xid.xid_ptr,
                                      timeout, flags)
        if status < 0:
            _raise_from_odpi()

    def tpc_commit(self, xid, bint one_phase):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            int status
        with nogil:
            status = dpiConn_tpcCommit(self._handle, thick_xid.xid_ptr,
                                       one_phase)
        if status < 0:
            _raise_from_odpi()

    def tpc_end(self, xid, uint32_t flags):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            int status
        with nogil:
            status = dpiConn_tpcEnd(self._handle, thick_xid.xid_ptr, flags)
        if status < 0:
            _raise_from_odpi()

    def tpc_forget(self, xid):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            int status
        with nogil:
            status = dpiConn_tpcForget(self._handle, thick_xid.xid_ptr)
        if status < 0:
            _raise_from_odpi()

    def tpc_prepare(self, xid):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            bint commit_needed
            int status
        with nogil:
            status = dpiConn_tpcPrepare(self._handle, thick_xid.xid_ptr,
                                        &commit_needed)
        if status < 0:
            _raise_from_odpi()
        return commit_needed

    def tpc_rollback(self, xid):
        cdef:
            ThickXid thick_xid = ThickXid(xid)
            int status
        with nogil:
            status = dpiConn_tpcRollback(self._handle, thick_xid.xid_ptr)
        if status < 0:
            _raise_from_odpi()
