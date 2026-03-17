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
# statement.pyx
#
# Cython file defining the Statement and BindInfo classes used to hold
# information about statements that are executed and any bind parameters that
# may be bound to those statements (embedded in thin_impl.pyx).
#------------------------------------------------------------------------------

cdef class BindInfo:

    cdef:
        uint32_t num_elements
        bint _is_return_bind
        uint8_t ora_type_num
        uint32_t buffer_size
        int16_t precision
        uint8_t bind_dir
        uint32_t size
        str _bind_name
        bint is_array
        int16_t scale
        uint8_t csfrm
        ThinVarImpl _bind_var_impl

    def __cinit__(self, str name, bint is_return_bind):
        self._bind_name = name
        self._is_return_bind = is_return_bind

    cdef BindInfo copy(self):
        return BindInfo(self._bind_name, self._is_return_bind)


cdef class StatementParser(BaseParser):

    cdef:
        bint returning_keyword_found

    cdef int _parse_bind_name(self, Statement stmt) except -1:
        """
        Bind variables are identified as follows:
        - Quoted and non-quoted bind names are allowed.
        - Quoted bind names can contain any characters.
        - Non-quoted bind names must begin with an alphabetic character.
        - Non-quoted bind names can only contain alphanumeric characters, the
          underscore, the dollar sign and the pound sign.
        - Non-quoted bind names cannot be Oracle Database Reserved Names (this
          is left to the server to detct and return an appropriate error).
        """
        cdef:
            bint quoted_name = False, in_bind = False, digits_only = False
            ssize_t start_pos = 0
            str bind_name
            Py_UCS4 ch
        self.temp_pos = self.pos + 1
        while self.temp_pos < self.num_chars:
            ch = self.get_current_char()
            if not in_bind:
                if cpython.Py_UNICODE_ISSPACE(ch):
                    self.temp_pos += 1
                    continue
                elif ch == '"':
                    quoted_name = True
                elif cpython.Py_UNICODE_ISDIGIT(ch):
                    digits_only = True
                elif not cpython.Py_UNICODE_ISALPHA(ch):
                    break
                in_bind = True
                start_pos = self.temp_pos
            elif digits_only and not cpython.Py_UNICODE_ISDIGIT(ch):
                self.pos = self.temp_pos - 1
                break
            elif quoted_name and ch == '"':
                self.pos = self.temp_pos
                break
            elif not digits_only and not quoted_name \
                    and not cpython.Py_UNICODE_ISALNUM(ch) \
                    and ch not in ('_', '$', '#'):
                self.pos = self.temp_pos - 1
                break
            self.temp_pos += 1
        if in_bind:
            if quoted_name:
                bind_name = stmt._sql[start_pos + 1:self.temp_pos]
            elif digits_only:
                bind_name = stmt._sql[start_pos:self.temp_pos]
            else:
                bind_name = stmt._sql[start_pos:self.temp_pos].upper()
            stmt._add_bind(bind_name)

    cdef int _parse_multiple_line_comment(self) except -1:
        """
        Multiple line comments consist of the characters /* followed by all
        characters up until */. This method is called when the first slash is
        detected and checks for the subsequent asterisk. If found, the comment
        is traversed and the current position is updated; otherwise, the
        current position is left untouched.
        """
        cdef:
            bint in_comment = False, exiting_comment = False
            Py_UCS4 ch
        self.temp_pos = self.pos + 1
        while self.temp_pos < self.num_chars:
            ch = self.get_current_char()
            if not in_comment:
                if ch != '*':
                    break
                in_comment = True
            elif ch == '*':
                exiting_comment = True
            elif exiting_comment:
                if ch == '/':
                    self.pos = self.temp_pos
                    break
                exiting_comment = False
            self.temp_pos += 1

    cdef int _parse_qstring(self) except -1:
        """
        Parses a q-string which consists of the characters "q" and a single
        quote followed by a start separator, any text that does not contain the
        end seprator and the end separator and ending quote. The following are
        examples that demonstrate this:
            - q'[...]'
            - q'{...}'
            - q'<...>'
            - q'(...)'
            - q'?...?' (where ? is any character)
        """
        cdef:
            bint exiting_qstring = False, in_qstring = False
            Py_UCS4 ch, sep = 0
        self.temp_pos += 1
        while self.temp_pos < self.num_chars:
            ch = self.get_current_char()
            if not in_qstring:
                if ch == '[':
                    sep = ']'
                elif ch == '{':
                    sep = '}'
                elif ch == '<':
                    sep = '>'
                elif ch == '(':
                    sep = ')'
                else:
                    sep = ch
                in_qstring = True
            elif not exiting_qstring and ch == sep:
                exiting_qstring = True
            elif exiting_qstring:
                if ch == "'":
                    self.pos = self.temp_pos
                    return 0
                elif ch != sep:
                    exiting_qstring = False
            self.temp_pos += 1
        errors._raise_err(errors.ERR_MISSING_ENDING_SINGLE_QUOTE)

    cdef int _parse_single_line_comment(self) except -1:
        """
        Single line comments consist of two dashes and all characters up to the
        next line break (or the end of the data). This method is called when
        the first dash is detected and checks for the subsequent dash. If
        found, the single line comment is traversed and the current position is
        updated; otherwise, the current position is left untouched.
        """
        cdef:
            bint in_comment = False
            Py_UCS4 ch
        self.temp_pos = self.pos + 1
        while self.temp_pos < self.num_chars:
            ch = self.get_current_char()
            if not in_comment:
                if ch != '-':
                    return 0
                in_comment = True
            elif cpython.Py_UNICODE_ISLINEBREAK(ch):
                break
            self.temp_pos += 1
        self.pos = self.temp_pos

    cdef int parse(self, Statement stmt) except -1:
        """
        Parses the SQL stored in the statement in order to determine the
        keyword that identifies the type of SQL being executed as well as a
        list of bind variable names. A check is also made for DML returning
        statements since the bind variables following the "INTO" keyword are
        treated differently from other bind variables.
        """
        cdef:
            bint initial_keyword_found = False, last_was_string = False
            Py_UCS4 ch, last_ch = 0, alpha_start_ch = 0
            ssize_t alpha_start_pos = 0, alpha_len
            bint last_was_alpha = False, is_alpha
            str keyword

        # initialization
        self.initialize(stmt._sql)

        # scan all characters in the string
        while self.pos < self.num_chars:
            self.temp_pos = self.pos
            ch = self.get_current_char()

            # look for certain keywords (initial keyword and the ones for
            # detecting DML returning statements
            is_alpha = cpython.Py_UNICODE_ISALPHA(ch)
            if is_alpha and not last_was_alpha:
                alpha_start_pos = self.pos
                alpha_start_ch = ch
            elif not is_alpha and last_was_alpha:
                alpha_len = self.pos - alpha_start_pos
                if not initial_keyword_found:
                    keyword = stmt._sql[alpha_start_pos:self.pos].upper()
                    stmt._determine_statement_type(keyword)
                    if stmt._is_ddl:
                        break
                    initial_keyword_found = True
                elif stmt._is_dml and not self.returning_keyword_found \
                        and alpha_len == 9 and alpha_start_ch in ('r', 'R'):
                    keyword = stmt._sql[alpha_start_pos:self.pos].upper()
                    if keyword == "RETURNING":
                        self.returning_keyword_found = True
                elif self.returning_keyword_found and alpha_len == 4 \
                        and alpha_start_ch in ('i', 'I'):
                    keyword = stmt._sql[alpha_start_pos:self.pos].upper()
                    if keyword == "INTO":
                        stmt._is_returning = True

            # need to keep track of whether the last token parsed was a string
            # (excluding whitespace) as if the last token parsed was a string
            # a following colon is not a bind variable but a part of the JSON
            # constant syntax
            if ch == "'":
                last_was_string = True
                if last_ch in ('q', 'Q'):
                    self._parse_qstring()
                else:
                    self.temp_pos += 1
                    self.parse_quoted_string(ch)
                    self.pos -= 1
            elif not cpython.Py_UNICODE_ISSPACE(ch):
                if ch == '-':
                    self._parse_single_line_comment()
                elif ch == '/':
                    self._parse_multiple_line_comment()
                elif ch == '"':
                    self.temp_pos += 1
                    self.parse_quoted_string(ch)
                    self.pos -= 1
                elif ch == ':' and not last_was_string:
                    self._parse_bind_name(stmt)
                last_was_string = False

            # advance to next character and track previous character
            self.pos += 1
            last_was_alpha = is_alpha
            last_ch = ch


cdef class Statement:

    cdef:
        str _sql
        bytes _sql_bytes
        uint32_t _sql_length
        uint16_t _cursor_id
        bint _is_query
        bint _is_plsql
        bint _is_dml
        bint _is_ddl
        bint _is_returning
        list _bind_info_list
        list _fetch_info_impls
        list _fetch_vars
        list _fetch_var_impls
        object _bind_info_dict
        object _last_output_type_handler
        uint32_t _num_columns
        bint _executed
        bint _binds_changed
        bint _no_prefetch
        bint _requires_define
        bint _return_to_cache
        bint _in_use

    cdef Statement copy(self):
        cdef:
            Statement copied_statement = Statement.__new__(Statement)
            object bind_info_dict
            BindInfo bind_info
        copied_statement._sql = self._sql
        copied_statement._sql_bytes = self._sql_bytes
        copied_statement._sql_length = self._sql_length
        copied_statement._is_query = self._is_query
        copied_statement._is_plsql = self._is_plsql
        copied_statement._is_dml = self._is_dml
        copied_statement._is_ddl = self._is_ddl
        copied_statement._is_returning = self._is_returning
        copied_statement._bind_info_list = \
                [bind_info.copy() for bind_info in self._bind_info_list]
        copied_statement._bind_info_dict = collections.OrderedDict()
        bind_info_dict = copied_statement._bind_info_dict
        for bind_info in copied_statement._bind_info_list:
            if bind_info._bind_name in bind_info_dict:
                bind_info_dict[bind_info._bind_name].append(bind_info)
            else:
                bind_info_dict[bind_info._bind_name] = [bind_info]
        copied_statement._return_to_cache = False
        return copied_statement

    cdef int _add_bind(self, str name) except -1:
        """
        Add bind information to the statement by examining the passed SQL for
        bind variable names.
        """
        cdef BindInfo info, orig_info
        if not self._is_plsql or name not in self._bind_info_dict:
            info = BindInfo(name, self._is_returning)
            self._bind_info_list.append(info)
            if info._bind_name in self._bind_info_dict:
                if self._is_returning:
                    orig_info = self._bind_info_dict[info._bind_name][-1]
                    if not orig_info._is_return_bind:
                        errors._raise_err(errors.ERR_DML_RETURNING_DUP_BINDS,
                                          name=info._bind_name)
                self._bind_info_dict[info._bind_name].append(info)
            else:
                self._bind_info_dict[info._bind_name] = [info]

    cdef _determine_statement_type(self, str sql_keyword):
        """
        Determine the type of the SQL statement by examining the first keyword
        found in the statement.
        """
        if sql_keyword in ("DECLARE", "BEGIN", "CALL"):
            self._is_plsql = True
        elif sql_keyword in ("SELECT", "WITH"):
            self._is_query = True
        elif sql_keyword in ("INSERT", "UPDATE", "DELETE", "MERGE"):
            self._is_dml = True
        elif sql_keyword in ("CREATE", "ALTER", "DROP", "GRANT", "REVOKE",
                             "ANALYZE", "AUDIT", "COMMENT", "TRUNCATE"):
            self._is_ddl = True

    cdef int _prepare(self, str sql) except -1:
        """
        Prepare the SQL for execution by determining the list of bind names
        that are found within it. The length of the SQL text is also calculated
        at this time.
        """
        cdef StatementParser parser = StatementParser.__new__(StatementParser)

        # retain normalized SQL (as string and bytes) as well as the length
        self._sql = sql
        self._sql_bytes = self._sql.encode()
        self._sql_length = <uint32_t> len(self._sql_bytes)

        # parse SQL and populate bind variable list (bind by position) and dict
        # (bind by name)
        self._bind_info_dict = collections.OrderedDict()
        self._bind_info_list = []
        parser.parse(self)

    cdef int _set_var(self, BindInfo bind_info, ThinVarImpl var_impl,
                      ThinCursorImpl cursor_impl) except -1:
        """
        Set the variable on the bind information and copy across metadata that
        will be used for binding. If the bind metadata has changed, mark the
        statement as requiring a full execute. In addition, binding a REF
        cursor also requires a full execute.
        """
        cdef object value
        if var_impl.dbtype._ora_type_num == TNS_DATA_TYPE_CURSOR:
            for value in var_impl._values:
                if value is not None and value._impl is cursor_impl:
                    errors._raise_err(errors.ERR_SELF_BIND_NOT_SUPPORTED)
            self._binds_changed = True
        if var_impl.dbtype._ora_type_num != bind_info.ora_type_num \
                or var_impl.size != bind_info.size \
                or var_impl.buffer_size != bind_info.buffer_size \
                or var_impl.precision != bind_info.precision \
                or var_impl.scale != bind_info.scale \
                or var_impl.is_array != bind_info.is_array \
                or var_impl.num_elements != bind_info.num_elements \
                or var_impl.dbtype._csfrm != bind_info.csfrm:
            bind_info.ora_type_num = var_impl.dbtype._ora_type_num
            bind_info.csfrm = var_impl.dbtype._csfrm
            bind_info.is_array = var_impl.is_array
            bind_info.num_elements = var_impl.num_elements
            bind_info.size = var_impl.size
            bind_info.buffer_size = var_impl.buffer_size
            bind_info.precision = var_impl.precision
            bind_info.scale = var_impl.scale
            self._binds_changed = True
        bind_info._bind_var_impl = var_impl

    cdef int clear_all_state(self) except -1:
        """
        Clears all state associated with the cursor.
        """
        self._fetch_vars = None
        self._fetch_info_impls = None
        self._fetch_var_impls = None
        self._executed = False
        self._binds_changed = False
        self._requires_define = False
        self._no_prefetch = False
        self._cursor_id = 0

    cdef bint requires_single_execute(self):
        """
        Returns a boolean indicating if the statement requires a single execute
        in order to be processed correctly by the server. If a PL/SQL block has
        not been executed before, the determination of input/output binds has
        not been completed and so a single execution is required in order to
        complete that determination.
        """
        return self._is_plsql and (self._cursor_id == 0 or self._binds_changed)
