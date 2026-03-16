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
# Cython file defining the base Cursor implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

@cython.freelist(20)
cdef class FetchInfoImpl:
    pass


cdef class BaseCursorImpl:

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef int _bind_values(self,
                          object cursor,
                          object type_handler,
                          object params,
                          uint32_t num_rows,
                          uint32_t row_num,
                          bint defer_type_assignment) except -1:
        """
        Internal method used for binding values.
        """
        if self.bind_vars is None:
            self.bind_vars = []
        if isinstance(params, dict):
            if self.bind_style is None:
                self.bind_style = dict
                self.bind_vars_by_name = {}
            elif self.bind_style is not dict:
                errors._raise_err(errors.ERR_MIXED_POSITIONAL_AND_NAMED_BINDS)
            self._bind_values_by_name(cursor, type_handler, <dict> params,
                                      num_rows, row_num, defer_type_assignment)
        elif cpython.PySequence_Check(params):
            if self.bind_style is None:
                self.bind_style = list
            elif self.bind_style is not list:
                errors._raise_err(errors.ERR_MIXED_POSITIONAL_AND_NAMED_BINDS)
            self._bind_values_by_position(cursor, type_handler, params,
                                          num_rows, row_num,
                                          defer_type_assignment)
        else:
            errors._raise_err(errors.ERR_WRONG_EXECUTEMANY_PARAMETERS_TYPE)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef int _bind_values_by_name(self,
                                  object cursor,
                                  object type_handler,
                                  dict params,
                                  uint32_t num_rows,
                                  uint32_t row_num,
                                  bint defer_type_assignment) except -1:
        """
        Internal method used for binding values by name.
        """
        cdef:
            BindVar bind_var
            object conn
            ssize_t pos
        conn = cursor.connection
        for name, value in params.items():
            bind_var = <BindVar> self.bind_vars_by_name.get(name)
            if bind_var is None:
                pos = len(self.bind_vars_by_name)
                if pos < len(self.bind_vars):
                    bind_var = <BindVar> self.bind_vars[pos]
                else:
                    bind_var = BindVar.__new__(BindVar)
                    self.bind_vars.append(bind_var)
                bind_var.name = name
                self.bind_vars_by_name[name] = bind_var
            bind_var._set_by_value(conn, self, cursor, value, type_handler,
                                   row_num, num_rows, defer_type_assignment)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef int _bind_values_by_position(self,
                                      object cursor,
                                      object type_handler,
                                      object params,
                                      uint32_t num_rows,
                                      uint32_t row_num,
                                      bint defer_type_assignment) except -1:
        """
        Internal method used for binding values by position.
        """
        cdef:
            BindVar bind_var
            object conn
            ssize_t i
        conn = cursor.connection
        for i, value in enumerate(params):
            if i < len(self.bind_vars):
                bind_var = <BindVar> self.bind_vars[i]
            else:
                bind_var = BindVar.__new__(BindVar)
                bind_var.pos = i + 1
                self.bind_vars.append(bind_var)
            bind_var._set_by_value(conn, self, cursor, value, type_handler,
                                   row_num, num_rows, defer_type_assignment)

    def _build_json_converter_fn(self):
        """
        Internal method for building a JSON converter function.
        """
        def converter(value):
            if isinstance(value, PY_TYPE_LOB):
                value = value.read()
            if isinstance(value, bytes):
                value = value.decode()
            if value:
                return json.loads(value)
        return converter

    cdef int _check_binds(self, uint32_t num_execs) except -1:
        """
        Checks that all binds are capable of handling the number of executions
        provided.
        """
        cdef BindVar bind_var
        for bind_var in self.bind_vars:
            if bind_var is None or bind_var.var_impl is None:
                continue
            if bind_var.var_impl.num_elements < num_execs:
                errors._raise_err(errors.ERR_INCORRECT_VAR_ARRAYSIZE,
                                  var_arraysize=bind_var.var_impl.num_elements,
                                  required_arraysize=num_execs)

    cdef int _close(self, bint in_del) except -1:
        """
        Internal method for closing the cursor.
        """
        raise NotImplementedError()

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef BaseVarImpl _create_fetch_var(self, object conn, object cursor,
                                       object type_handler, bint
                                       uses_fetch_info, ssize_t pos,
                                       FetchInfoImpl fetch_info):
        """
        Create the fetch variable for the given position and fetch information.
        The output type handler is consulted, if present, to make any necessary
        adjustments.
        """
        cdef:
            object var, pub_fetch_info
            BaseConnImpl conn_impl
            BaseVarImpl var_impl
            uint32_t db_type_num

        # add the fetch info to the list used for handling the cursor
        # description attribute
        self.fetch_info_impls[pos] = fetch_info

        # if an output type handler is specified, call it; the output type
        # handler should return a variable or None; the value None implies that
        # the default processing should take place just as if no output type
        # handler was defined
        if type_handler is not None:
            if uses_fetch_info:
                pub_fetch_info = PY_TYPE_FETCHINFO._from_impl(fetch_info)
                var = type_handler(cursor, pub_fetch_info)
            else:
                var = type_handler(cursor, fetch_info.name, fetch_info.dbtype,
                                   fetch_info.size, fetch_info.precision,
                                   fetch_info.scale)
            if var is not None:
                self._verify_var(var)
                var_impl = var._impl
                var_impl._fetch_info = fetch_info
                if var_impl.num_elements < self._fetch_array_size:
                    var_impl.num_elements = self._fetch_array_size
                    var_impl._finalize_init()
                self.fetch_vars[pos] = var
                self.fetch_var_impls[pos] = var_impl
                return var_impl

        # otherwise, create a new variable using the provided fetch information
        var_impl = self._create_var_impl(conn)
        var_impl.num_elements = self._fetch_array_size
        var_impl.dbtype = fetch_info.dbtype
        var_impl.objtype = fetch_info.objtype
        var_impl.name = fetch_info.name
        var_impl.size = fetch_info.size
        var_impl.precision = fetch_info.precision
        var_impl.scale = fetch_info.scale
        var_impl.nulls_allowed = fetch_info.nulls_allowed
        var_impl._fetch_info = fetch_info

        # adjust the variable based on the defaults specified by the user, if
        # applicable
        db_type_num = var_impl.dbtype.num
        if db_type_num == DB_TYPE_NUM_NUMBER:
            if C_DEFAULTS.fetch_decimals:
                var_impl._preferred_num_type = NUM_TYPE_DECIMAL
            elif var_impl.scale == 0 \
                    or (var_impl.scale == -127 and var_impl.precision == 0):
                var_impl._preferred_num_type = NUM_TYPE_INT
        elif fetch_info.is_oson and db_type_num != DB_TYPE_NUM_JSON:
            conn_impl = self._get_conn_impl()
            var_impl.dbtype = DB_TYPE_LONG_RAW
            var_impl.outconverter = conn_impl.decode_oson
        elif fetch_info.is_json and db_type_num != DB_TYPE_NUM_JSON:
            var_impl.outconverter = self._build_json_converter_fn()
        elif not C_DEFAULTS.fetch_lobs:
            if db_type_num == DB_TYPE_NUM_BLOB:
                var_impl.dbtype = DB_TYPE_LONG_RAW
            elif db_type_num == DB_TYPE_NUM_CLOB:
                var_impl.dbtype = DB_TYPE_LONG
            elif db_type_num == DB_TYPE_NUM_NCLOB:
                var_impl.dbtype = DB_TYPE_LONG_NVARCHAR

        # finalize variable and store in arrays
        var_impl._finalize_init()
        self.fetch_var_impls[pos] = var_impl
        return var_impl

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef object _create_row(self):
        """
        Internal method for creating a row from the fetched data.
        """
        cdef:
            Py_ssize_t i, num_vars
            BaseVarImpl var_impl
            object row, value
        num_vars = cpython.PyList_GET_SIZE(self.fetch_var_impls)
        row = cpython.PyTuple_New(num_vars)
        for i in range(num_vars):
            var_impl = self.fetch_var_impls[i]
            value = var_impl._get_scalar_value(self._buffer_index)
            cpython.Py_INCREF(value)
            cpython.PyTuple_SET_ITEM(row, i, value)
        if self.rowfactory is not None:
            row = self.rowfactory(*row)
        self._buffer_index += 1
        self._buffer_rowcount -= 1
        self.rowcount += 1
        return row

    cdef BaseVarImpl _create_var_impl(self, object conn):
        """
        Internal method for creating a variable.
        """
        raise NotImplementedError()

    cdef int _fetch_rows(self, object cursor) except -1:
        """
        Internal method used for fetching rows from a cursor.
        """
        raise NotImplementedError()

    cdef BaseConnImpl _get_conn_impl(self):
        """
        Internal method used to return the connection implementation associated
        with the cursor implementation.
        """
        raise NotImplementedError()

    cdef object _get_input_type_handler(self):
        """
        Return the input type handler to use for the cursor. If one is not
        directly defined on the cursor then the one defined on the connection
        is used instead.
        """
        cdef BaseConnImpl conn_impl
        if self.inputtypehandler is not None:
            return self.inputtypehandler
        conn_impl = self._get_conn_impl()
        return conn_impl.inputtypehandler

    def _get_oci_attr(self, uint32_t attr_num, uint32_t attr_type):
        errors._raise_not_supported("getting a cursor OCI attribute")

    cdef object _get_output_type_handler(self, bint *uses_fetch_info):
        """
        Return the output type handler to use for the cursor. If one is not
        directly defined on the cursor then the one defined on the connection
        is used instead.
        """
        cdef:
            BaseConnImpl conn_impl
            object type_handler
        if self.outputtypehandler is not None:
            type_handler = self.outputtypehandler
        else:
            conn_impl = self._get_conn_impl()
            type_handler = conn_impl.outputtypehandler
        if type_handler is not None:
            try:
                sig = inspect.signature(type_handler)
                uses_fetch_info[0] = (len(sig.parameters) == 2)
            except (ValueError, TypeError):
                uses_fetch_info[0] = False
        return type_handler

    cdef int _init_fetch_vars(self, uint32_t num_columns) except -1:
        """
        Initializes the fetch variable lists in preparation for creating the
        fetch variables used in fetching rows from the database.
        """
        self.fetch_info_impls = [None] * num_columns
        self.fetch_vars = [None] * num_columns
        self.fetch_var_impls = [None] * num_columns

    cdef bint _is_plsql(self):
        """
        Internal method that indicates whether the currently prepared statement
        is a PL/SQL statement or not.
        """
        raise NotImplementedError()

    cdef int _perform_binds(self, object conn, uint32_t num_execs) except -1:
        """
        Perform all binds on the cursor.
        """
        cdef:
            BindVar bind_var
            ssize_t i
        for i, bind_var in enumerate(self.bind_vars):
            if bind_var is not None and bind_var.var_impl is not None:
                bind_var.var_impl._bind(conn, self, num_execs, bind_var.name,
                                        bind_var.pos)

    cdef int _prepare(self, str statement, str tag,
                      bint cache_statement) except -1:
        """
        Prepares a statement for execution.
        """
        self.statement = statement
        self.rowfactory = None
        self.fetch_vars = None
        if not self.set_input_sizes:
            self.bind_vars = None
            self.bind_vars_by_name = None
            self.bind_style = None

    def _prepare_for_execute(self, object cursor, str statement,
                             object parameters, object keyword_parameters):
        """
        Internal method for preparing a statement for execution.
        """
        cdef:
            bint prepare_needed

        # verify parameters
        if statement is None and self.statement is None:
            errors._raise_err(errors.ERR_NO_STATEMENT)
        if keyword_parameters:
            if parameters:
                errors._raise_err(errors.ERR_ARGS_AND_KEYWORD_ARGS)
            parameters = keyword_parameters
        elif parameters is not None and not isinstance(
            parameters, (list, tuple, dict)
        ):
            errors._raise_err(errors.ERR_WRONG_EXECUTE_PARAMETERS_TYPE)
        prepare_needed = statement and statement != self.statement
        if (
            not (prepare_needed and not self.set_input_sizes)
            and self.bind_vars is not None
            and parameters is not None
        ):
            if (
                self.bind_style is dict and not isinstance(parameters, dict)
                or self.bind_style is not dict and isinstance(parameters, dict)
            ):
                self.set_input_sizes = False
                errors._raise_err(errors.ERR_MIXED_POSITIONAL_AND_NAMED_BINDS)

        # prepare statement, if necessary
        try:
            if prepare_needed:
                self._prepare(statement, None, True)
        finally:
            self.set_input_sizes = False

        # perform bind
        if parameters is not None:
            self.bind_one(cursor, parameters)

        # clear any warning and reset rowcount
        self.warning = None
        self.rowcount = 0

    def _prepare_for_executemany(self, object cursor, str statement,
                                 object parameters):
        """
        Internal method for preparing a statement for execution multiple times.
        """

        # prepare statement, if necessary
        if statement is None and self.statement is None:
            errors._raise_err(errors.ERR_NO_STATEMENT)
        try:
            if statement is not None and statement != self.statement:
                self._prepare(statement, None, True)
        finally:
            self.set_input_sizes = False

        # perform bind, if applicable
        if isinstance(parameters, int):
            num_execs = parameters
            if self.bind_vars is not None:
                self._check_binds(num_execs)
        elif isinstance(parameters, list):
            num_execs = len(parameters)
            if parameters:
                self.bind_many(cursor, parameters)
        else:
            errors._raise_err(errors.ERR_WRONG_EXECUTEMANY_PARAMETERS_TYPE)

        # clear any warning and reset rowcount
        self.warning = None
        self.rowcount = 0

        return num_execs

    cdef int _reset_bind_vars(self, uint32_t num_rows) except -1:
        """
        Reset all of the existing bind variables. If any bind variables don't
        have enough space to store the number of rows specified, expand and
        then reinitialize that bind variable.
        """
        cdef:
            BindVar bind_var
            ssize_t i
        if self.bind_vars is not None:
            for i in range(len(self.bind_vars)):
                bind_var = <BindVar> self.bind_vars[i]
                if bind_var.var_impl is not None:
                    bind_var.var_impl._on_reset_bind(num_rows)
                bind_var.has_value = False

    def _set_oci_attr(self, uint32_t attr_num, uint32_t attr_type,
                      object value):
        errors._raise_not_supported("setting a cursor OCI attribute")

    cdef int _verify_var(self, object var) except -1:
        """
        Internal method used for verifying if an outputtypehandler returns a
        valid var object.
        """
        if not isinstance(var, PY_TYPE_VAR):
            errors._raise_err(errors.ERR_EXPECTING_VAR)
        if self.arraysize > var.num_elements:
            errors._raise_err(errors.ERR_INCORRECT_VAR_ARRAYSIZE,
                              var_arraysize=var.num_elements,
                              required_arraysize=self.arraysize)

    cdef int bind_many(self, object cursor, list parameters) except -1:
        """
        Internal method used for binding multiple rows of data.
        """
        cdef:
            bint defer_type_assignment
            ssize_t i, num_rows
            object params_row
        type_handler = self._get_input_type_handler()
        num_rows = len(parameters)
        self._reset_bind_vars(num_rows)
        for i, params_row in enumerate(parameters):
            defer_type_assignment = (i < num_rows - 1)
            self._bind_values(cursor, type_handler, params_row, num_rows, i,
                              defer_type_assignment)

    cdef int bind_one(self, object cursor, object parameters) except -1:
        """
        Internal method used for binding a single row of data.
        """
        cdef:
            bint defer_type_assignment = False
            uint32_t row_num = 0, num_rows = 1
            ssize_t num_bind_vars, pos
            object name, value
            BindVar bind_var
            dict dict_params
        type_handler = self._get_input_type_handler()
        self._reset_bind_vars(num_rows)
        self._bind_values(cursor, type_handler, parameters, num_rows, row_num,
                          defer_type_assignment)

    def close(self, bint in_del=False):
        """
        Closes the cursor and makes it unusable for further operations.
        """
        self.bind_vars = None
        self.bind_vars_by_name = None
        self.bind_style = None
        self.fetch_vars = None
        self._close(in_del)

    def create_var(self, object conn, object typ, uint32_t size=0,
                   uint32_t num_elements=1, object inconverter=None,
                   object outconverter=None, str encoding_errors=None,
                   bint bypass_decode=False, bint is_array=False,
                   bint convert_nulls=False):
        cdef BaseVarImpl var_impl
        var_impl = self._create_var_impl(conn)
        var_impl._set_type_info_from_type(typ)
        var_impl.size = size
        var_impl.num_elements = num_elements
        var_impl.inconverter = inconverter
        var_impl.outconverter = outconverter
        var_impl.bypass_decode = bypass_decode
        var_impl.encoding_errors = encoding_errors
        var_impl.is_array = is_array
        var_impl.convert_nulls = convert_nulls
        var_impl._finalize_init()
        return PY_TYPE_VAR._from_impl(var_impl)

    def execute(self, cursor):
        errors._raise_not_supported("executing a statement")

    def executemany(self, cursor, num_execs, batcherrors, arraydmlrowcounts):
        errors._raise_not_supported("executing a statement in batch")

    def fetch_next_row(self, cursor):
        """
        Internal method used for fetching the next row from a cursor.
        """
        if self._buffer_rowcount == 0 and self._more_rows_to_fetch:
            self._fetch_rows(cursor)
        if self._buffer_rowcount > 0:
            return self._create_row()

    def get_array_dml_row_counts(self):
        errors._raise_not_supported("getting a list of array DML row counts")

    def get_batch_errors(self):
        errors._raise_not_supported("getting a list of batch errors")

    def get_bind_names(self):
        errors._raise_not_supported("getting a list of bind variable names")

    def get_bind_vars(self):
        """
        Return a list (when binding by position) or a dictionary (when binding
        by name) of the bind variables associated with the cursor.
        """
        cdef:
            BindVar bind_var
            ssize_t i
        if self.bind_vars is None:
            return []
        for bind_var in self.bind_vars:
            if bind_var.var is None and bind_var.var_impl is not None:
                bind_var.var = PY_TYPE_VAR._from_impl(bind_var.var_impl)
        if self.bind_style is list:
            return [bind_var.var for bind_var in self.bind_vars]
        return dict([(bind_var.name, bind_var.var) \
                for bind_var in self.bind_vars])

    def get_fetch_vars(self):
        """
        Return a list of fetch variables. Initially the list contains all
        empty values except where a fetch type handler was used. This will
        populate any remaining fetch variables that are needed.
        """
        cdef:
            BaseVarImpl var_impl
            ssize_t i
        if self.fetch_vars is not None:
            for i, var in enumerate(self.fetch_vars):
                if var is None:
                    var_impl = <BaseVarImpl> self.fetch_var_impls[i]
                    self.fetch_vars[i] = PY_TYPE_VAR._from_impl(var_impl)
        return self.fetch_vars

    def get_implicit_results(self, connection):
        errors._raise_not_supported("getting implicit results from PL/SQL")

    def get_lastrowid(self):
        errors._raise_not_supported(
            "getting the rowid of the row last modified"
        )

    def is_query(self, cursor):
        errors._raise_not_supported(
            "determining if the cursor last executed a query"
        )

    def parse(self, cursor):
        errors._raise_not_supported("parsing a statement without executing it")

    def prepare(self, str statement, str tag, bint cache_statement):
        """
        Prepares a statement for execution.
        """
        self._prepare(statement, tag, cache_statement)

    def scroll(self, conn, value, mode):
        """
        Scrolls a scrollable cursor.
        """
        errors._raise_not_supported("scrolling a scrollable cursor")

    def setinputsizes(self, object conn, tuple args, dict kwargs):
        """
        Sets type information for bind variables in advance of executing a
        statement (and binding values).
        """
        cdef:
            object name, value
            BindVar bind_var
            ssize_t pos
        self.bind_vars = []
        self.set_input_sizes = True
        if kwargs:
            self.bind_style = dict
            self.bind_vars_by_name = {}
            for name, value in kwargs.items():
                bind_var = BindVar.__new__(BindVar)
                self.bind_vars.append(bind_var)
                self.bind_vars_by_name[name] = bind_var
                bind_var._set_by_type(conn, self, value)
                bind_var.name = name
        else:
            self.bind_style = list
            for pos, value in enumerate(args):
                bind_var = BindVar.__new__(BindVar)
                self.bind_vars.append(bind_var)
                bind_var._set_by_type(conn, self, value)
                bind_var.pos = pos + 1
        return self.get_bind_vars()
