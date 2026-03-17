#------------------------------------------------------------------------------
# Copyright (c) 2024, Oracle and/or its affiliates.
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
# statement_cache.pyx
#
# Cython file defining the StatementCache class used to manage cached
# statements (embedded in thin_impl.pyx).
#------------------------------------------------------------------------------

cdef class StatementCache:

    cdef:
        object _cached_statements
        object _lock
        uint32_t _max_size
        uint32_t _max_cursors
        array.array _cursors_to_close
        ssize_t _num_cursors_to_close
        set _open_cursors

    cdef int _add_cursor_to_close(self, Statement stmt) except -1:
        """
        Add the statement's cursor to the list of cursors that need to be
        closed.
        """
        if stmt._cursor_id != 0:
            self._cursors_to_close[self._num_cursors_to_close] = \
                    stmt._cursor_id
            self._num_cursors_to_close += 1
        self._open_cursors.remove(stmt)

    cdef int _adjust_cache(self) except -1:
        """
        Adjust the cache so that no more than the maximum number of statements
        are cached.
        """
        cdef Statement stmt
        while len(self._cached_statements) > self._max_size:
            stmt = <Statement> self._cached_statements.popitem(last=False)[1]
            if stmt._in_use:
                stmt._return_to_cache = False
            else:
                self._add_cursor_to_close(stmt)

    cdef int clear_cursor(self, Statement stmt) except -1:
        """
        Clears the particular cursor but retains it in the list of open
        cursors. This is for cases where the cursor id needs to be cleared but
        the cursor itself is still going to be used.
        """
        self._cursors_to_close[self._num_cursors_to_close] = stmt._cursor_id
        self._num_cursors_to_close += 1
        stmt._cursor_id = 0
        stmt._executed = False

    cdef int clear_open_cursors(self) except -1:
        """
        Clears the list of open cursors and removes the list of cursors that
        need to be closed. This is required when a DRCP session change has
        taken place as the cursor ID values are invalidated.
        """
        cdef:
            set new_open_cursors = set()
            Statement stmt
        with self._lock:
            for stmt in self._open_cursors:
                stmt.clear_all_state()
                if stmt._in_use or stmt._return_to_cache:
                    new_open_cursors.add(stmt)
            self._open_cursors = new_open_cursors
            self._num_cursors_to_close = 0

    cdef Statement get_statement(self, str sql, bint cache_statement,
                                 bint force_new):
        """
        Get a statement from the statement cache, or prepare a new statement
        for use. If a statement is already in use or the statement is not
        supposed to be cached, a copy will be made (and not returned to the
        cache).
        """
        cdef Statement stmt = None
        with self._lock:
            if sql is not None:
                stmt = self._cached_statements.get(sql)
            if stmt is None:
                stmt = Statement.__new__(Statement)
                if sql is not None:
                    stmt._prepare(sql)
                if cache_statement and not stmt._is_ddl and self._max_size > 0:
                    stmt._return_to_cache = True
                    self._cached_statements[sql] = stmt
                    self._adjust_cache()
                self._open_cursors.add(stmt)
            elif force_new or stmt._in_use:
                if not cache_statement:
                    self._add_cursor_to_close(stmt)
                    del self._cached_statements[sql]
                stmt = stmt.copy()
                self._open_cursors.add(stmt)
            elif not cache_statement:
                del self._cached_statements[sql]
                stmt._return_to_cache = False
            else:
                self._cached_statements.move_to_end(sql)
        stmt._in_use = True
        return stmt

    cdef int initialize(self, uint32_t max_size,
                         uint32_t max_cursors) except -1:
        """
        Initialize the statement cache.
        """
        if max_cursors == 0:
            self._max_size = 0
            self._max_cursors = 1
        else:
            self._max_size = max_size
            self._max_cursors = max_cursors
        self._cached_statements = collections.OrderedDict()
        self._lock = threading.Lock()
        self._open_cursors = set()
        self._cursors_to_close = array.array('I')
        array.resize(self._cursors_to_close, self._max_cursors)
        self._num_cursors_to_close = 0

    cdef int resize(self, uint32_t new_size) except -1:
        """
        Resizes the statement cache to the new value.
        """
        with self._lock:
            self._max_size = new_size
            self._adjust_cache()

    cdef int return_statement(self, Statement stmt) except -1:
        """
        Return the statement to the statement cache, if applicable. If the
        statement must not be returned to the statement cache, add the cursor
        id to the list of cursor ids to close on the next round trip to the
        database. Clear all bind variables and fetch variables in order to
        ensure that unnecessary references are not retained.
        """
        cdef:
            ThinVarImpl var_impl
            BindInfo bind_info
        if stmt._bind_info_list is not None:
            for bind_info in stmt._bind_info_list:
                bind_info._bind_var_impl = None
        if stmt._fetch_var_impls is not None:
            for var_impl in stmt._fetch_var_impls:
                var_impl._values = [None] * var_impl.num_elements
        with self._lock:
            if stmt._return_to_cache:
                stmt._in_use = False
            else:
                self._add_cursor_to_close(stmt)

    cdef int write_cursors_to_close(self, WriteBuffer buf) except -1:
        """
        Write the list of cursors to close to the buffer.
        """
        cdef:
            unsigned int *cursor_ids
            ssize_t i
        with self._lock:
            buf.write_ub4(self._num_cursors_to_close)
            cursor_ids = self._cursors_to_close.data.as_uints
            for i in range(self._num_cursors_to_close):
                buf.write_ub4(cursor_ids[i])
            self._num_cursors_to_close = 0
