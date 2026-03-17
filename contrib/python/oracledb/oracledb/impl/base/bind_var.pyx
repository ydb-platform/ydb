#------------------------------------------------------------------------------
# Copyright (c) 2022, Oracle and/or its affiliates.
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
# bind_var.pyx
#
# Cython file defining the BindVar implementation class (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

@cython.freelist(20)
cdef class BindVar:

    cdef int _create_var_from_type(self, object conn,
                                   BaseCursorImpl cursor_impl,
                                   object value) except -1:
        """
        Creates a variable given type information. This may be supplied as a
        list of size 2 (type, num_elements) which internally creates an array
        variable, or as an integer which internally creates a string of the
        given length, or as a database type, API type or Python type.
        """
        cdef BaseVarImpl var_impl
        var_impl = cursor_impl._create_var_impl(conn)
        var_impl.num_elements = 1
        if isinstance(value, list):
            if len(value) != 2:
                errors._raise_err(errors.ERR_WRONG_ARRAY_DEFINITION)
            var_impl._set_type_info_from_type(value[0])
            var_impl.num_elements = value[1]
            var_impl.is_array = True
        elif isinstance(value, int):
            var_impl.dbtype = DB_TYPE_VARCHAR
            var_impl.size = value
        else:
            var_impl._set_type_info_from_type(value)
        var_impl._finalize_init()
        self.var_impl = var_impl
        if isinstance(value, PY_TYPE_DB_OBJECT_TYPE):
            self.var = PY_TYPE_VAR._from_impl(self.var_impl, value)

    cdef int _create_var_from_value(self, object conn,
                                    BaseCursorImpl cursor_impl, object value,
                                    uint32_t num_elements) except -1:
        """
        Creates a variable using the value as a template.
        """
        cdef:
            bint is_plsql = cursor_impl._is_plsql()
            BaseVarImpl var_impl
        var_impl = cursor_impl._create_var_impl(conn)
        if not isinstance(value, list):
            var_impl.num_elements = num_elements
            var_impl._set_type_info_from_value(value, is_plsql)
        else:
            var_impl.is_array = True
            var_impl.num_elements = max(num_elements, len(value))
            for element in value:
                if element is not None:
                    var_impl._set_type_info_from_value(element, is_plsql)
            if var_impl.dbtype is None:
                var_impl.dbtype = DB_TYPE_VARCHAR
                var_impl.size = 1
        var_impl._finalize_init()
        self.var_impl = var_impl

    cdef int _set_by_type(self, object conn, BaseCursorImpl cursor_impl,
                          object typ) except -1:
        """
        Sets the bind variable information given a type.
        """
        if typ is not None:
            if isinstance(typ, PY_TYPE_VAR):
                self.var = typ
                self.var_impl = typ._impl
            else:
                self._create_var_from_type(conn, cursor_impl, typ)

    cdef int _set_by_value(self, object conn, BaseCursorImpl cursor_impl,
                           object cursor, object value, object type_handler,
                           uint32_t row_num, uint32_t num_elements,
                           bint defer_type_assignment) except -1:
        """
        Sets the bind variable information given a value. The row number
        supplied is used as the offset into the variable value array. Type
        assignment is deferred for None values if specified. Once a value that
        is not None is set, an exception is raised for any non-compliant values
        that are seen after that.
        """
        cdef:
            bint was_set = True
            object var

        # a variable can be set directly in which case nothing further needs to
        # be done!
        if isinstance(value, PY_TYPE_VAR):
            if value is not self.var:
                self.var = value
                self.var_impl = value._impl
            return 0

        # if a variable already exists check to see if the value can be set on
        # that variable; an exception is raised if a value has been previously
        # set on that bind variable; otherwise, the variable is replaced with a
        # new one
        if self.var_impl is not None:
            if self.has_value:
                return self.var_impl._check_and_set_value(row_num, value, NULL)
            self.var_impl._check_and_set_value(row_num, value, &was_set)
            if was_set:
                self.has_value = True
                return 0
            self.var_impl = None
            self.var = None

        # a new variable needs to be created; if the value is null (None),
        # however, and type assignment is deferred, nothing to do!
        if value is None and defer_type_assignment:
            return 0

        # if an input type handler is specified, call it; the input type
        # handler should return a variable or None; the value None implies
        # that the default processing should take place just as if no input
        # type handler was defined
        if type_handler is not None:
            var = type_handler(cursor, value, num_elements)
            if var is not None:
                if not isinstance(var, PY_TYPE_VAR):
                    errors._raise_err(errors.ERR_EXPECTING_VAR)
                self.var = var
                self.var_impl = var._impl
                self.var_impl._check_and_set_value(row_num, value, NULL)
                self.has_value = True
                return 0

        # otherwise, if no type handler exists or the type handler returned
        # the value None, create a new variable deriving type information
        # from the value that is being set
        self._create_var_from_value(conn, cursor_impl, value, num_elements)
        self.var_impl._check_and_set_value(row_num, value, NULL)
        self.has_value = True

    def get_value(self, uint32_t pos):
        """
        Internal method for getting the value of a variable at a given
        position.
        """
        if self.var_impl is not None:
            return self.var_impl.get_value(pos)
