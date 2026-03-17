#------------------------------------------------------------------------------
# Copyright (c) 2020, 2022, Oracle and/or its affiliates.
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
# lob.pyx
#
# Cython file defining the thick Lob implementation class (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------

cdef class ThickLobImpl(BaseLobImpl):
    cdef:
        dpiLob *_handle

    @staticmethod
    cdef ThickLobImpl _create(ThickConnImpl conn_impl, DbType dbtype,
                             dpiLob *handle):
        cdef:
            ThickLobImpl impl = ThickLobImpl.__new__(ThickLobImpl)
            int status
        impl.dbtype = dbtype
        if handle == NULL:
            with nogil:
                status = dpiConn_newTempLob(conn_impl._handle, dbtype.num,
                                            &handle)
            if status < 0:
                _raise_from_odpi()
        elif dpiLob_addRef(handle) < 0:
            _raise_from_odpi()
        impl._handle = handle
        return impl

    def close(self):
        """
        Internal method for closing a LOB that was opened earlier.
        """
        cdef int status
        with nogil:
            status = dpiLob_closeResource(self._handle)
        if status < 0:
            _raise_from_odpi()

    def file_exists(self):
        """
        Internal method for returning whether the file referenced by a BFILE
        exists.
        """
        cdef:
            bint exists
            int status
        with nogil:
            status = dpiLob_getFileExists(self._handle, &exists)
        if status < 0:
            _raise_from_odpi()
        return exists

    def free_lob(self):
        """
        Internal method for releasing the handle
        """
        if self._handle != NULL:
            dpiLob_release(self._handle)

    def get_chunk_size(self):
        """
        Internal method for returning the chunk size of the LOB.
        """
        cdef uint32_t chunk_size
        if dpiLob_getChunkSize(self._handle, &chunk_size) < 0:
            _raise_from_odpi()
        return chunk_size

    def get_file_name(self):
        """
        Internal method for returning a 2-tuple constaining the directory alias
        and file name of a BFILE type LOB.
        """
        cdef:
            uint32_t dir_alias_len, file_name_len
            const char *dir_alias
            const char *file_name
            int status
        with nogil:
            status = dpiLob_getDirectoryAndFileName(self._handle, &dir_alias,
                                                    &dir_alias_len,
                                                    &file_name, &file_name_len)
        if status < 0:
            _raise_from_odpi()
        return (dir_alias[:dir_alias_len].decode(),
                file_name[:file_name_len].decode())

    def get_is_open(self):
        """
        Internal method for returning whether the LOB is open or not.
        """
        cdef:
            bint is_open
            int status
        with nogil:
            status = dpiLob_getIsResourceOpen(self._handle, &is_open)
        if status < 0:
            _raise_from_odpi()
        return is_open

    def get_max_amount(self):
        """
        Internal method for returning the maximum amount that can be read.
        """
        return self.get_size()

    def get_size(self):
        """
        Internal method for returning the size of a LOB.
        """
        cdef uint64_t size
        if dpiLob_getSize(self._handle, &size) < 0:
            _raise_from_odpi()
        return size

    def open(self):
        """
        Internal method for opening a LOB.
        """
        cdef int status
        with nogil:
            status = dpiLob_openResource(self._handle)
        if status < 0:
            _raise_from_odpi()

    def read(self, uint64_t offset, uint64_t amount):
        """
        Internal method for reading a portion (or all) of the data in the LOB.
        """
        cdef:
            uint64_t buf_size
            object result
            char *buf
            int status
        if dpiLob_getBufferSize(self._handle, amount, &buf_size) < 0:
            _raise_from_odpi()
        buf = <char*> cpython.PyMem_Malloc(buf_size)
        with nogil:
            status = dpiLob_readBytes(self._handle, offset, amount, buf,
                                      &buf_size)
        try:
            if status < 0:
                _raise_from_odpi()
            result = buf[:buf_size]
            if self.dbtype.num == DPI_ORACLE_TYPE_CLOB \
                    or self.dbtype.num == DPI_ORACLE_TYPE_NCLOB:
                result = result.decode()
            return result
        finally:
            cpython.PyMem_Free(buf)

    def set_file_name(self, str dir_alias, str name):
        """
        Internal method for setting the directory alias and file name
        associated with a BFILE LOB.
        """
        cdef:
            bytes dir_alias_bytes, name_bytes
            uint32_t dir_alias_len, name_len
            const char *dir_alias_ptr
            const char *name_ptr
            int status
        dir_alias_bytes = dir_alias.encode()
        dir_alias_ptr = dir_alias_bytes
        dir_alias_len = <uint32_t> len(dir_alias_bytes)
        name_bytes = name.encode()
        name_ptr = name_bytes
        name_len = <uint32_t> len(name_bytes)
        with nogil:
            status = dpiLob_setDirectoryAndFileName(self._handle,
                                                    dir_alias_ptr,
                                                    dir_alias_len,
                                                    name_ptr, name_len)
        if status < 0:
            _raise_from_odpi()

    def trim(self, uint64_t new_size):
        """
        Internal method for trimming the data in the LOB to the new size
        """
        cdef int status
        with nogil:
            status = dpiLob_trim(self._handle, new_size)
        if status < 0:
            _raise_from_odpi()

    def write(self, object value, uint64_t offset):
        """
        Internal method for writing data to the LOB object.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            int status
        buf.set_value(value)
        with nogil:
            status = dpiLob_writeBytes(self._handle, offset, buf.ptr,
                                       buf.length)
        if status < 0:
            _raise_from_odpi()
