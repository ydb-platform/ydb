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
# cursor.pyx
#
# Cython file defining the thick Cursor implementation class (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------


cdef class ThickCursorImpl(BaseCursorImpl):
    cdef:
        ThickConnImpl _conn_impl
        bint _is_implicit_cursor
        bint _fixup_ref_cursor
        dpiStmtInfo _stmt_info
        dpiStmt *_handle
        str _tag

    def __cinit__(self, conn_impl):
        self._conn_impl = conn_impl

    def __dealloc__(self):
        if self._handle != NULL:
            dpiStmt_release(self._handle)

    cdef int _close(self, bint in_del) except -1:
        """
        Internal method for closing the cursor.
        """
        if self._handle != NULL:
            if not in_del and self._conn_impl._handle != NULL \
                    and not self._is_implicit_cursor:
                if dpiStmt_close(self._handle, NULL, 0) < 0:
                    _raise_from_odpi()
            dpiStmt_release(self._handle)
            self._handle = NULL

    cdef BaseVarImpl _create_var_impl(self, object conn):
        """
        Internal method for creating a variable.
        """
        cdef ThickVarImpl var_impl = ThickVarImpl.__new__(ThickVarImpl)
        var_impl._conn = conn
        var_impl._conn_impl = self._conn_impl
        return var_impl

    cdef int _define_var(self, object conn, object cursor, object type_handler,
                         bint uses_fetch_info, ssize_t pos) except -1:
        """
        Internal method that creates the variable using the query info (unless
        an output type handler has been specified) and then performs the define
        in ODPI-C.
        """
        cdef:
            ThickDbObjectTypeImpl typ_impl
            dpiDataTypeInfo *type_info
            FetchInfoImpl fetch_info
            ThickConnImpl conn_impl
            dpiQueryInfo query_info
            ThickVarImpl var_impl
            uint32_t i, temp_len
            str key, value

        # build FetchInfoImpl based on query info provided by ODPI-C
        if dpiStmt_getQueryInfo(self._handle, pos + 1, &query_info) < 0:
            _raise_from_odpi()
        type_info = &query_info.typeInfo
        fetch_info = FetchInfoImpl.__new__(FetchInfoImpl)
        fetch_info.dbtype = DbType._from_num(type_info.oracleTypeNum)
        if type_info.sizeInChars > 0:
            fetch_info.size = type_info.sizeInChars
        else:
            fetch_info.size = type_info.clientSizeInBytes
        fetch_info.buffer_size = type_info.clientSizeInBytes
        fetch_info.name = query_info.name[:query_info.nameLength].decode()
        fetch_info.scale = type_info.scale + type_info.fsPrecision
        fetch_info.precision = type_info.precision
        fetch_info.nulls_allowed = query_info.nullOk
        fetch_info.is_json = type_info.isJson
        fetch_info.is_oson = type_info.isOson
        if type_info.domainSchema:
            temp_len = type_info.domainSchemaLength
            fetch_info.domain_schema = \
                    type_info.domainSchema[:temp_len].decode()
        if type_info.domainName:
            fetch_info.domain_name = \
                    type_info.domainName[:type_info.domainNameLength].decode()
        if type_info.numAnnotations > 0:
            fetch_info.annotations = {}
            for i in range(type_info.numAnnotations):
                temp_len = type_info.annotations[i].keyLength
                key = type_info.annotations[i].key[:temp_len].decode()
                temp_len = type_info.annotations[i].valueLength
                value = type_info.annotations[i].value[:temp_len].decode()
                fetch_info.annotations[key] = value
        if type_info.objectType != NULL:
            typ_impl = ThickDbObjectTypeImpl._from_handle(self._conn_impl,
                                                          type_info.objectType)
            fetch_info.objtype = typ_impl
        if fetch_info.dbtype.num == DPI_ORACLE_TYPE_VECTOR:
            fetch_info.vector_dimensions = type_info.vectorDimensions
            fetch_info.vector_format = type_info.vectorFormat
            fetch_info.vector_flags = type_info.vectorFlags

        # create variable and call define in ODPI-C
        var_impl = <ThickVarImpl> self._create_fetch_var(
            conn, cursor, type_handler, uses_fetch_info, pos, fetch_info
        )
        if dpiStmt_define(self._handle, pos + 1, var_impl._handle) < 0:
            _raise_from_odpi()

    cdef int _fetch_rows(self, object cursor) except -1:
        """
        Internal method for fetching rows from a cursor.
        """
        cdef:
            uint32_t temp_buffer_row_index, num_rows_in_buffer
            bint more_rows_to_fetch
            ThickVarImpl var_impl
            int status
        with nogil:
            status = dpiStmt_fetchRows(self._handle,
                                       self._fetch_array_size,
                                       &temp_buffer_row_index,
                                       &num_rows_in_buffer,
                                       &more_rows_to_fetch)
        if status < 0:
            _raise_from_odpi()
        self._buffer_index = 0
        self._buffer_rowcount = num_rows_in_buffer
        self._more_rows_to_fetch = more_rows_to_fetch

    cdef BaseConnImpl _get_conn_impl(self):
        """
        Internal method used to return the connection implementation associated
        with the cursor implementation.
        """
        return self._conn_impl

    cdef bint _is_plsql(self):
        return <bint> self._stmt_info.isPLSQL

    def _get_oci_attr(self, uint32_t attr_num, uint32_t attr_type):
        """
        Internal method for getting the value of an OCI attribute on the
        cursor.
        """
        cdef:
            dpiDataBuffer value
            uint32_t value_len
        if dpiStmt_getOciAttr(self._handle, attr_num, &value, &value_len) < 0:
            _raise_from_odpi()
        return _convert_oci_attr_to_python(attr_type, &value, value_len)

    cdef int _perform_define(self, object cursor,
                             uint32_t num_query_cols) except -1:
        """
        Internal method for performing defines. At this point it is assumed
        that the statement executed was in fact a query.
        """
        cdef:
            ThickCursorImpl cursor_impl = <ThickCursorImpl> cursor._impl
            object var, type_handler, conn
            ThickVarImpl var_impl
            bint uses_fetch_info
            ssize_t i

        # initialize fetching variables; these are used to reduce the number of
        # times that the GIL is released/acquired -- as there is a significant
        # amount of overhead in doing so
        self._buffer_rowcount = 0
        self._more_rows_to_fetch = True

        # if fetch variables already exist, nothing to do (the same statement
        # is being executed and therefore all defines have already been
        # performed
        if self.fetch_vars is not None:
            return 0

        # populate list to contain fetch variables that are created
        self._fetch_array_size = self.arraysize
        self._init_fetch_vars(num_query_cols)
        type_handler = self._get_output_type_handler(&uses_fetch_info)
        conn = cursor.connection
        for i in range(num_query_cols):
            self._define_var(conn, cursor, type_handler, uses_fetch_info, i)

    cdef int _prepare(self, str statement, str tag,
                      bint cache_statement) except -1:
        """
        Internal method used for preparing statements for execution.
        """
        cdef:
            uint32_t statement_bytes_length, tag_bytes_length = 0
            ThickConnImpl conn_impl = self._conn_impl
            bytes statement_bytes, tag_bytes
            const char *tag_ptr = NULL
            const char *statement_ptr
            int status
        BaseCursorImpl._prepare(self, statement, tag, cache_statement)
        statement_bytes = statement.encode()
        statement_ptr = <const char*> statement_bytes
        statement_bytes_length = <uint32_t> len(statement_bytes)
        if tag is not None:
            self._tag = tag
            tag_bytes = tag.encode()
            tag_bytes_length = <uint32_t> len(tag_bytes)
            tag_ptr = <const char*> tag_bytes
        with nogil:
            if self._handle != NULL:
                dpiStmt_release(self._handle)
                self._handle = NULL
            status = dpiConn_prepareStmt(conn_impl._handle, self.scrollable,
                                         statement_ptr, statement_bytes_length,
                                         tag_ptr, tag_bytes_length,
                                         &self._handle)
            if status == DPI_SUCCESS and not cache_statement:
                status = dpiStmt_deleteFromCache(self._handle)
            if status == DPI_SUCCESS:
                status = dpiStmt_getInfo(self._handle, &self._stmt_info)
            if status == DPI_SUCCESS and self._stmt_info.isQuery:
                status = dpiStmt_setFetchArraySize(self._handle,
                                                   self.arraysize)
                if status == DPI_SUCCESS \
                        and self.prefetchrows != DPI_DEFAULT_PREFETCH_ROWS:
                    status = dpiStmt_setPrefetchRows(self._handle,
                                                     self.prefetchrows)
        if status < 0:
            _raise_from_odpi()

    def _set_oci_attr(self, uint32_t attr_num, uint32_t attr_type,
                      object value):
        """
        Internal method for setting the value of an OCI attribute on the
        cursor.
        """
        cdef:
            StringBuffer str_buf = StringBuffer()
            void *oci_value = NULL
            dpiDataBuffer oci_buf
            uint32_t oci_len = 0
        _convert_python_to_oci_attr(value, attr_type, str_buf, &oci_buf,
                                    &oci_value, &oci_len)
        if dpiStmt_setOciAttr(self._handle, attr_num, oci_value, oci_len) < 0:
            _raise_from_odpi()

    cdef int _transform_binds(self) except -1:
        cdef:
            ThickVarImpl var_impl
            uint32_t num_elements
            BindVar bind_var
        if self.bind_vars is not None:
            for bind_var in self.bind_vars:
                var_impl = <ThickVarImpl> bind_var.var_impl
                if var_impl.is_array:
                    if dpiVar_getNumElementsInArray(var_impl._handle,
                                                    &num_elements) < 0:
                        _raise_from_odpi()
                    var_impl.num_elements_in_array = num_elements

    def execute(self, cursor):
        """
        Internal method for executing a statement.
        """
        cdef:
            uint32_t mode, num_query_cols
            dpiErrorInfo error_info
            uint64_t rowcount = 0
            int status
        if self.bind_vars is not None:
            self._perform_binds(cursor.connection, 1)
        if self._conn_impl.autocommit:
            mode = DPI_MODE_EXEC_COMMIT_ON_SUCCESS
        else:
            mode = DPI_MODE_EXEC_DEFAULT
        with nogil:
            status = dpiStmt_execute(self._handle, mode, &num_query_cols)
            if status == DPI_SUCCESS:
                dpiContext_getError(driver_info.context, &error_info)
                if not self._stmt_info.isPLSQL:
                    status = dpiStmt_getRowCount(self._handle, &rowcount)
        if status < 0:
            _raise_from_odpi()
        elif error_info.isWarning:
            self.warning = _create_new_from_info(&error_info)
        self.rowcount = rowcount
        if num_query_cols > 0:
            self._perform_define(cursor, num_query_cols)
        elif self._stmt_info.isReturning or self._stmt_info.isPLSQL:
            self._transform_binds()

    def executemany(self, cursor, num_execs, batcherrors, arraydmlrowcounts):
        """
        Internal method for executing a statement multiple times.
        """
        cdef:
            uint32_t mode, num_execs_int = num_execs
            dpiErrorInfo error_info
            uint64_t rowcount = 0
            int status

        if self._conn_impl.autocommit:
            mode = DPI_MODE_EXEC_COMMIT_ON_SUCCESS
        else:
            mode = DPI_MODE_EXEC_DEFAULT
        if arraydmlrowcounts:
            mode |= DPI_MODE_EXEC_ARRAY_DML_ROWCOUNTS
        if batcherrors:
            mode |= DPI_MODE_EXEC_BATCH_ERRORS

        if self.bind_vars is not None:
            self._perform_binds(cursor.connection, num_execs_int)

        if num_execs_int > 0:
            with nogil:
                status = dpiStmt_executeMany(self._handle, mode, num_execs_int)
                dpiContext_getError(driver_info.context, &error_info)
                dpiStmt_getRowCount(self._handle, &rowcount)
            if not self._stmt_info.isPLSQL:
                self.rowcount = rowcount
            if status < 0:
                error = _create_new_from_info(&error_info)
                if self._stmt_info.isPLSQL and error_info.offset == 0:
                    error.offset = rowcount
                raise error.exc_type(error)
            elif error_info.isWarning:
                self.warning = _create_new_from_info(&error_info)
            if self._stmt_info.isReturning or self._stmt_info.isPLSQL:
                self._transform_binds()

    def get_array_dml_row_counts(self):
        """
        Internal method for returning a list of array DML row counts for the
        last operation executed.
        """
        cdef:
            uint32_t num_row_count, i
            uint64_t *rowcount
            int status

        status = dpiStmt_getRowCounts(self._handle, &num_row_count, &rowcount)
        if status < 0:
            _raise_from_odpi()

        result = []
        for i in range(num_row_count):
            result.append(rowcount[i])
        return result

    def get_batch_errors(self):
        """
        Internal method for returning a list of batch errors.
        """
        cdef:
            uint32_t num_errors, i
            dpiErrorInfo *errors

        if dpiStmt_getBatchErrorCount(self._handle, &num_errors) < 0:
            _raise_from_odpi()
        if num_errors == 0:
            return []

        errors = <dpiErrorInfo*> \
                cpython.PyMem_Malloc(num_errors * sizeof(dpiErrorInfo))

        try:
            if dpiStmt_getBatchErrors(self._handle, num_errors, errors) < 0:
                _raise_from_odpi()
            result = cpython.PyList_New(num_errors)
            for i in range(num_errors):
                error = _create_new_from_info(&errors[i])
                cpython.Py_INCREF(error)
                cpython.PyList_SET_ITEM(result, i, error)
        finally:
            cpython.PyMem_Free(errors)

        return result

    def get_bind_names(self):
        cdef:
            uint32_t *name_lengths = NULL
            const char **names = NULL
            uint32_t num_binds, i
            ssize_t num_bytes
            list result
        if dpiStmt_getBindCount(self._handle, &num_binds) < 0:
            _raise_from_odpi()
        if num_binds == 0:
            return []
        try:
            num_bytes = num_binds * sizeof(char*)
            names = <const char**> cpython.PyMem_Malloc(num_bytes)
            num_bytes = num_binds * sizeof(uint32_t)
            name_lengths = <uint32_t*> cpython.PyMem_Malloc(num_bytes)
            if dpiStmt_getBindNames(self._handle, &num_binds, names,
                                    name_lengths) < 0:
                _raise_from_odpi()
            result = [None] * num_binds
            for i in range(num_binds):
                result[i] = names[i][:name_lengths[i]].decode()
            return result
        finally:
            if names:
                cpython.PyMem_Free(names)
            if name_lengths:
                cpython.PyMem_Free(name_lengths)

    def get_implicit_results(self, connection):
        cdef:
            ThickCursorImpl child_cursor_impl
            object child_cursor
            dpiStmt *child_stmt
            list result = []
        if self._handle == NULL:
            errors._raise_err(errors.ERR_NO_STATEMENT_EXECUTED)
        while True:
            if dpiStmt_getImplicitResult(self._handle, &child_stmt) < 0:
                _raise_from_odpi()
            if child_stmt == NULL:
                break
            child_cursor = connection.cursor()
            child_cursor_impl = child_cursor._impl
            child_cursor_impl._handle = child_stmt
            child_cursor_impl._fixup_ref_cursor = True
            child_cursor_impl._is_implicit_cursor = True
            result.append(child_cursor)
        return result

    def get_lastrowid(self):
        """
        Internal method for returning the rowid of the row that was last
        modified by an operation.
        """
        cdef:
            uint32_t rowid_length
            const char *rowid_ptr
            dpiRowid *rowid
        if self._handle is not NULL:
            if dpiStmt_getLastRowid(self._handle, &rowid) < 0:
                _raise_from_odpi()
            if rowid:
                if dpiRowid_getStringValue(rowid, &rowid_ptr,
                                           &rowid_length) < 0:
                    _raise_from_odpi()
                return rowid_ptr[:rowid_length].decode()

    def is_query(self, cursor):
        cdef uint32_t num_query_cols
        if self._fixup_ref_cursor:
            self._fetch_array_size = self.arraysize
            if dpiStmt_setFetchArraySize(self._handle,
                                         self._fetch_array_size) < 0:
                _raise_from_odpi()
            if dpiStmt_getNumQueryColumns(self._handle, &num_query_cols) < 0:
                _raise_from_odpi()
            self._perform_define(cursor, num_query_cols)
            self._fixup_ref_cursor = False
        return self.fetch_vars is not None

    def parse(self, cursor):
        """
        Internal method for parsing a statement.
        """
        cdef:
            uint32_t mode, num_query_cols
            int status
        if self._stmt_info.isQuery:
            mode = DPI_MODE_EXEC_DESCRIBE_ONLY
        else:
            mode = DPI_MODE_EXEC_PARSE_ONLY
        with nogil:
            status = dpiStmt_execute(self._handle, mode, &num_query_cols)
        if status < 0:
            _raise_from_odpi()
        if num_query_cols > 0:
            self._perform_define(cursor, num_query_cols)

    def scroll(self, object conn, int32_t offset, object mode):
        cdef:
            uint32_t temp_buffer_row_index = 0, num_rows_in_buffer = 0
            bint more_rows_to_fetch = False
            ThickVarImpl var_impl
            uint32_t int_mode = 0
            int status
        if mode == "relative":
            int_mode = DPI_MODE_FETCH_RELATIVE
        elif mode == "absolute":
            int_mode = DPI_MODE_FETCH_ABSOLUTE
        elif mode == "first":
            int_mode = DPI_MODE_FETCH_FIRST
        elif mode == "last":
            int_mode = DPI_MODE_FETCH_LAST
        else:
            errors._raise_err(errors.ERR_WRONG_SCROLL_MODE)
        with nogil:
            status = dpiStmt_scroll(self._handle, int_mode, offset,
                                    0 - self._buffer_rowcount)
            if status == 0:
                status = dpiStmt_fetchRows(self._handle,
                                           self._fetch_array_size,
                                           &temp_buffer_row_index,
                                           &num_rows_in_buffer,
                                           &more_rows_to_fetch)
            if status == 0:
                status = dpiStmt_getRowCount(self._handle, &self.rowcount)
        if status < 0:
            _raise_from_odpi()
        self._buffer_index = temp_buffer_row_index
        self._buffer_rowcount = num_rows_in_buffer
        self._more_rows_to_fetch = more_rows_to_fetch
        self.rowcount -= num_rows_in_buffer
