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
# soda.pyx
#
# Cython file defining the thick implementation SODA classes (embedded in
# thick_impl.pyx).
#------------------------------------------------------------------------------

cdef class ThickSodaDbImpl(BaseSodaDbImpl):
    cdef:
        dpiSodaDb* _handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiSodaDb_release(self._handle)

    cdef int _get_flags(self, uint32_t* flags) except -1:
        self._conn._verify_connected()
        if self._conn.autocommit:
            flags[0] = DPI_SODA_FLAGS_ATOMIC_COMMIT
        else:
            flags[0] = DPI_SODA_FLAGS_DEFAULT

    def create_collection(self, str name, str metadata, bint map_mode):
        """
        Internal method for creating a collection.
        """
        cdef:
            StringBuffer metadata_buf = StringBuffer()
            StringBuffer name_buf = StringBuffer()
            ThickSodaCollImpl coll_impl
            uint32_t flags
            int status
        name_buf.set_value(name)
        metadata_buf.set_value(metadata)
        self._get_flags(&flags)
        if map_mode:
            flags |= DPI_SODA_FLAGS_CREATE_COLL_MAP
        coll_impl = ThickSodaCollImpl.__new__(ThickSodaCollImpl)
        coll_impl._db_impl = self
        with nogil:
            status = dpiSodaDb_createCollection(self._handle, name_buf.ptr,
                                                name_buf.length,
                                                metadata_buf.ptr,
                                                metadata_buf.length, flags,
                                                &coll_impl._handle)
        if status < 0:
            _raise_from_odpi()
        coll_impl._get_name()
        return coll_impl

    def create_document(self, bytes content, str key, str media_type):
        """
        Internal method for creating a document containing binary or encoded
        text data.
        """
        cdef:
            StringBuffer media_type_buf = StringBuffer()
            StringBuffer content_buf = StringBuffer()
            StringBuffer key_buf = StringBuffer()
            ThickSodaDocImpl doc_impl
        content_buf.set_value(content)
        key_buf.set_value(key)
        media_type_buf.set_value(media_type)
        doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
        if dpiSodaDb_createDocument(self._handle, key_buf.ptr, key_buf.length,
                                    content_buf.ptr, content_buf.length,
                                    media_type_buf.ptr, media_type_buf.length,
                                    DPI_SODA_FLAGS_DEFAULT,
                                    &doc_impl._handle) < 0:
            _raise_from_odpi()
        return doc_impl

    def create_json_document(self, object content, str key):
        """
        Internal method for creating a document containing JSON.
        """
        cdef:
            StringBuffer key_buf = StringBuffer()
            JsonBuffer json_buf = JsonBuffer()
            ThickSodaDocImpl doc_impl
        key_buf.set_value(key)
        json_buf.from_object(content)
        doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
        if dpiSodaDb_createJsonDocument(self._handle, key_buf.ptr,
                                        key_buf.length, &json_buf._top_node,
                                        DPI_SODA_FLAGS_DEFAULT,
                                        &doc_impl._handle) < 0:
            _raise_from_odpi()
        return doc_impl

    def get_collection_names(self, str start_name, uint32_t limit):
        """
        Internal method for getting the list of collection names.
        """
        cdef:
            StringBuffer start_name_buf = StringBuffer()
            dpiStringList names
            uint32_t flags
            int status
        start_name_buf.set_value(start_name)
        self._get_flags(&flags)
        with nogil:
            status = dpiSodaDb_getCollectionNames(self._handle,
                                                  start_name_buf.ptr,
                                                  start_name_buf.length,
                                                  limit, flags, &names)
        if status < 0:
            _raise_from_odpi()
        return _string_list_to_python(&names)

    def open_collection(self, str name):
        """
        Internal method for opening a collection.
        """
        cdef:
            StringBuffer name_buf = StringBuffer()
            ThickSodaCollImpl coll_impl
            uint32_t flags
            int status
        name_buf.set_value(name)
        self._get_flags(&flags)
        coll_impl = ThickSodaCollImpl.__new__(ThickSodaCollImpl)
        coll_impl._db_impl = self
        with nogil:
            status = dpiSodaDb_openCollection(self._handle, name_buf.ptr,
                                              name_buf.length, flags,
                                              &coll_impl._handle)
        if status < 0:
            _raise_from_odpi()
        if coll_impl._handle != NULL:
            coll_impl._get_name()
            return coll_impl


cdef class ThickSodaCollImpl(BaseSodaCollImpl):
    cdef:
        ThickSodaDbImpl _db_impl
        dpiSodaColl* _handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiSodaColl_release(self._handle)

    cdef int _get_name(self) except -1:
        """
        Internal method for getting the name of the collection.
        """
        cdef:
            uint32_t name_len
            const char *name
        if dpiSodaColl_getName(self._handle, &name, &name_len) < 0:
            _raise_from_odpi()
        self.name = name[:name_len].decode()

    cdef int _process_options(self, dpiSodaOperOptions *options,
                              const char *ptr, uint32_t length) except -1:
        """
        Internal method for populating the SODA operations structure with the
        information provided by the user.
        """
        if dpiContext_initSodaOperOptions(driver_info.context, options) < 0:
            _raise_from_odpi()
        options.hint = ptr
        options.hintLength = length

    def create_index(self, str spec):
        """
        Internal method for creating an index on a collection.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            uint32_t flags
            int status
        buf.set_value(spec)
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_createIndex(self._handle, buf.ptr, buf.length,
                                             flags)
        if status < 0:
            _raise_from_odpi()

    def drop(self):
        """
        Internal method for dropping a collection.
        """
        cdef:
            bint is_dropped
            uint32_t flags
        self._db_impl._get_flags(&flags)
        if dpiSodaColl_drop(self._handle, flags, &is_dropped) < 0:
            _raise_from_odpi()
        return is_dropped

    def drop_index(self, str name, bint force):
        """
        Internal method for dropping an index on a collection.
        """
        cdef:
            StringBuffer buf = StringBuffer()
            bint is_dropped
            uint32_t flags
            int status
        buf.set_value(name)
        self._db_impl._get_flags(&flags)
        if force:
            flags |= DPI_SODA_FLAGS_INDEX_DROP_FORCE
        with nogil:
            status = dpiSodaColl_dropIndex(self._handle, buf.ptr, buf.length,
                                           flags, &is_dropped)
        if status < 0:
            _raise_from_odpi()
        return is_dropped

    def get_count(self, object op):
        """
        Internal method for getting the count of documents matching the
        criteria.
        """
        cdef:
            ThickSodaOpImpl options = ThickSodaOpImpl._from_op(op)
            uint64_t count
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_getDocCount(self._handle, &options._options,
                                             flags, &count)
        if status < 0:
            _raise_from_odpi()
        return count

    def get_cursor(self, object op):
        """
        Internal method for getting a cursor which will return the documents
        matching the criteria.
        """
        cdef:
            ThickSodaOpImpl options = ThickSodaOpImpl._from_op(op)
            ThickSodaDocCursorImpl cursor_impl
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        cursor_impl = ThickSodaDocCursorImpl.__new__(ThickSodaDocCursorImpl)
        cursor_impl._db_impl = self._db_impl
        with nogil:
            status = dpiSodaColl_find(self._handle, &options._options, flags,
                                      &cursor_impl._handle)
        if status < 0:
            _raise_from_odpi()
        return cursor_impl

    def get_data_guide(self):
        """
        Internal method for getting the data guide for a collection.
        """
        cdef:
            ThickSodaDocImpl doc_impl
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
        with nogil:
            status = dpiSodaColl_getDataGuide(self._handle, flags,
                                              &doc_impl._handle)
        if status < 0:
            _raise_from_odpi()
        if doc_impl._handle != NULL:
            return doc_impl

    def get_metadata(self):
        """
        Internal method for getting the metadata for a collection.
        """
        cdef:
            uint32_t value_len
            const char* value
        if dpiSodaColl_getMetadata(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        return value[:value_len].decode()

    def get_one(self, object op):
        """
        Internal method for getting a document matching the criteria.
        """
        cdef:
            ThickSodaOpImpl options = ThickSodaOpImpl._from_op(op)
            ThickSodaDocImpl doc_impl
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
        with nogil:
            status = dpiSodaColl_findOne(self._handle, &options._options,
                                         flags, &doc_impl._handle)
        if status < 0:
            _raise_from_odpi()
        if doc_impl._handle != NULL:
            return doc_impl

    def insert_many(self, list doc_impls, str hint, bint return_docs):
        """
        Internal method for inserting many documents into a collection at once.
        """
        cdef:
            dpiSodaDoc **output_handles = NULL
            uint32_t i, num_docs, flags
            ThickSodaDocImpl doc_impl
            list output_doc_impls
            dpiSodaDoc **handles
            ssize_t num_bytes
            dpiSodaOperOptions options
            dpiSodaOperOptions *options_ptr = NULL
            StringBuffer hint_buf = StringBuffer()
            int status
        num_docs = <uint32_t> len(doc_impls)
        num_bytes = num_docs * sizeof(dpiSodaDoc *)
        handles = <dpiSodaDoc**> cpython.PyMem_Malloc(num_bytes)
        if return_docs:
            output_handles = <dpiSodaDoc**> _calloc(num_docs,
                                                    sizeof(dpiSodaDoc*))
            if hint is not None:
                hint_buf.set_value(hint)
                options_ptr = &options
                self._process_options(&options, hint_buf.ptr, hint_buf.length)
        for i, doc_impl in enumerate(doc_impls):
            handles[i] = doc_impl._handle
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_insertManyWithOptions(self._handle, num_docs,
                                                       handles, options_ptr,
                                                       flags, output_handles)
        if status < 0:
            _raise_from_odpi()
        if return_docs:
            output_doc_impls = []
            for i in range(num_docs):
                doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
                doc_impl._handle = output_handles[i]
                output_doc_impls.append(doc_impl)
            return output_doc_impls

    def insert_one(self, ThickSodaDocImpl doc_impl, str hint, bint return_doc):
        """
        Internal method for inserting a single document into a collection.
        """
        cdef:
            dpiSodaDoc **output_handle = NULL
            ThickSodaDocImpl output_doc_impl
            uint32_t flags
            dpiSodaOperOptions options
            dpiSodaOperOptions *options_ptr = NULL
            StringBuffer hint_buf = StringBuffer()
            int status
        if return_doc:
            output_doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
            output_handle = &output_doc_impl._handle
            if hint is not None:
               hint_buf.set_value(hint)
               options_ptr = &options
               self._process_options(&options, hint_buf.ptr, hint_buf.length)
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_insertOneWithOptions(self._handle,
                                                      doc_impl._handle,
                                                      options_ptr, flags,
                                                      output_handle)
        if status < 0:
            _raise_from_odpi()
        if return_doc:
            return output_doc_impl

    def list_indexes(self):
        """
        Internal method for getting the list of indexes on a collection.
        """
        cdef:
            dpiStringList indexes
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_listIndexes(self._handle, flags, &indexes)
        if status < 0:
            _raise_from_odpi()
        return _string_list_to_python(&indexes)

    def remove(self, object op):
        """
        Internal method for removing all of the documents matching the
        criteria.
        """
        cdef:
            ThickSodaOpImpl options = ThickSodaOpImpl._from_op(op)
            uint64_t count
            uint32_t flags
            int status
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_remove(self._handle, &options._options, flags,
                                        &count)
        if status < 0:
            _raise_from_odpi()
        return count

    def replace_one(self, object op, ThickSodaDocImpl doc_impl,
                    bint return_doc):
        """
        Internal method for replacing the document matching the criteria with
        the supplied coument.
        """
        cdef:
            ThickSodaOpImpl options = ThickSodaOpImpl._from_op(op)
            dpiSodaDoc **output_handle = NULL
            ThickSodaDocImpl output_doc_impl
            uint32_t flags
            bint replaced
            int status
        if return_doc:
            output_doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
            output_handle = &output_doc_impl._handle
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_replaceOne(self._handle, &options._options,
                                            doc_impl._handle, flags, &replaced,
                                            output_handle)
        if status < 0:
            _raise_from_odpi()
        if return_doc:
            return output_doc_impl
        return replaced

    def save(self, ThickSodaDocImpl doc_impl, str hint, bint return_doc):
        """
        Internal method for saving a document into the collection.
        """
        cdef:
            dpiSodaDoc **output_handle = NULL
            ThickSodaDocImpl output_doc_impl
            uint32_t flags
            dpiSodaOperOptions options
            dpiSodaOperOptions *options_ptr = NULL
            StringBuffer hint_buf = StringBuffer()
            int status
        if return_doc:
            output_doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
            output_handle = &output_doc_impl._handle
            if hint is not None:
               hint_buf.set_value(hint)
               options_ptr = &options
               self._process_options(&options, hint_buf.ptr, hint_buf.length)
        self._db_impl._get_flags(&flags)
        with nogil:
            status = dpiSodaColl_saveWithOptions(self._handle,
                                                 doc_impl._handle,
                                                 options_ptr, flags,
                                                 output_handle)
        if status < 0:
            _raise_from_odpi()
        if return_doc:
            return output_doc_impl

    def truncate(self):
        """
        Internal method for truncating the collection (removing all documents
        from it).
        """
        cdef int status
        with nogil:
            status = dpiSodaColl_truncate(self._handle)
        if status < 0:
            _raise_from_odpi()


cdef class ThickSodaDocImpl(BaseSodaDocImpl):
    cdef:
        dpiSodaDoc* _handle

    def __dealloc__(self):
        if self._handle != NULL:
            dpiSodaDoc_release(self._handle)

    def get_content(self):
        """
        Internal method for returning the content of the document.
        """
        cdef:
            object out_content = None
            str out_encoding = None
            const char *encoding
            uint32_t content_len
            const char *content
            dpiJson *json
            bint is_json
        if dpiSodaDoc_getIsJson(self._handle, &is_json) < 0:
            _raise_from_odpi()
        if is_json:
            if dpiSodaDoc_getJsonContent(self._handle, &json) < 0:
                _raise_from_odpi()
            out_content = _convert_json_to_python(json)
        else:
            if dpiSodaDoc_getContent(self._handle, &content, &content_len,
                                    &encoding) < 0:
                _raise_from_odpi()
            if content != NULL:
                out_content = content[:content_len]
            if encoding != NULL:
                out_encoding = encoding.decode()
            else:
                out_encoding = "UTF-8"
        return (out_content, out_encoding)

    def get_created_on(self):
        """
        Internal method for getting the date the document was created.
        """
        cdef:
            uint32_t value_len
            const char *value
        if dpiSodaDoc_getCreatedOn(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        if value_len > 0:
            return value[:value_len].decode()

    def get_key(self):
        """
        Internal method for getting the key of the document.
        """
        cdef:
            uint32_t value_len
            const char *value
        if dpiSodaDoc_getKey(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        if value_len > 0:
            return value[:value_len].decode()

    def get_last_modified(self):
        """
        Internal method for getting the date the document was last modified.
        """
        cdef:
            uint32_t value_len
            const char *value
        if dpiSodaDoc_getLastModified(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        if value_len > 0:
            return value[:value_len].decode()

    def get_media_type(self):
        """
        Internal method for getting the media type of the document.
        """
        cdef:
            uint32_t value_len
            const char *value
        if dpiSodaDoc_getMediaType(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        if value_len > 0:
            return value[:value_len].decode()

    def get_version(self):
        """
        Internal method for getting the version of the document.
        """
        cdef:
            uint32_t value_len
            const char *value
        if dpiSodaDoc_getVersion(self._handle, &value, &value_len) < 0:
            _raise_from_odpi()
        if value_len > 0:
            return value[:value_len].decode()


cdef class ThickSodaDocCursorImpl(BaseSodaDocCursorImpl):
    cdef:
        dpiSodaDocCursor* _handle
        ThickSodaDbImpl _db_impl

    def __dealloc__(self):
        if self._handle != NULL:
            dpiSodaDocCursor_release(self._handle)

    def close(self):
        """
        Internal method for closing the cursor.
        """
        cdef int status
        with nogil:
            status = dpiSodaDocCursor_close(self._handle)
        if status < 0:
            _raise_from_odpi()

    def get_next_doc(self):
        """
        Internal method for getting the next document from the cursor.
        """
        cdef:
            ThickSodaDocImpl doc_impl
            int status
        doc_impl = ThickSodaDocImpl.__new__(ThickSodaDocImpl)
        with nogil:
            status = dpiSodaDocCursor_getNext(self._handle,
                                              DPI_SODA_FLAGS_DEFAULT,
                                              &doc_impl._handle)
        if status < 0:
            _raise_from_odpi()
        if doc_impl._handle != NULL:
            return doc_impl


cdef class ThickSodaOpImpl:
    cdef:
        dpiSodaOperOptions _options
        const char** _key_values
        uint32_t* _key_lengths
        list _buffers

    def __dealloc__(self):
        if self._key_values != NULL:
            cpython.PyMem_Free(self._key_values)
        if self._key_lengths != NULL:
            cpython.PyMem_Free(self._key_lengths)

    cdef int _add_buf(self, object value, const char **ptr,
                      uint32_t *length) except -1:
        cdef StringBuffer buf = StringBuffer()
        buf.set_value(value)
        self._buffers.append(buf)
        ptr[0] = buf.ptr
        length[0] = buf.length

    @staticmethod
    cdef ThickSodaOpImpl _from_op(object op):
        """
        Internal method for creating a SODA operations implementation object
        given the object supplied by the user.
        """
        cdef:
            ThickSodaOpImpl impl = ThickSodaOpImpl.__new__(ThickSodaOpImpl)
            dpiSodaOperOptions *options
            ssize_t num_bytes
            uint32_t i
        impl._buffers = []
        options = &impl._options
        if dpiContext_initSodaOperOptions(driver_info.context, options) < 0:
            _raise_from_odpi()
        if op._keys:
            options.numKeys = <uint32_t> len(op._keys)
            num_bytes = options.numKeys * sizeof(char*)
            impl._key_values = <const char**> cpython.PyMem_Malloc(num_bytes)
            num_bytes = options.numKeys * sizeof(uint32_t)
            impl._key_lengths = <uint32_t*> cpython.PyMem_Malloc(num_bytes)
            options.keys = impl._key_values
            options.keyLengths = impl._key_lengths
            for i in range(options.numKeys):
                impl._add_buf(op._keys[i], &impl._key_values[i],
                              &impl._key_lengths[i])
        if op._key is not None:
            impl._add_buf(op._key, &options.key, &options.keyLength)
        if op._version is not None:
            impl._add_buf(op._version, &options.version,
                          &options.versionLength)
        if op._filter is not None:
            impl._add_buf(op._filter, &options.filter, &options.filterLength)
        if op._hint is not None:
            impl._add_buf(op._hint, &options.hint, &options.hintLength)
        if op._skip is not None:
            options.skip = op._skip
        if op._limit is not None:
            options.limit = op._limit
        if op._fetch_array_size is not None:
            options.fetchArraySize = op._fetch_array_size
        if op._lock:
            options.lock = True
        return impl
