#------------------------------------------------------------------------------
# Copyright (c) 2022, 2024, Oracle and/or its affiliates.
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
# dbobject.pyx
#
# Cython file defining the thin DbObjectType, DbObjectAttr and DbObject
# implementation classes (embedded in thin_impl.pyx).
#------------------------------------------------------------------------------

@cython.final
cdef class DbObjectPickleBuffer(GrowableBuffer):

    cdef int _read_raw_bytes_and_length(self, const char_type **ptr,
                                        ssize_t *num_bytes) except -1:
        """
        Helper function that processes the length (if needed) and then acquires
        the specified number of bytes from the buffer.
        """
        cdef uint32_t extended_num_bytes
        if num_bytes[0] == TNS_LONG_LENGTH_INDICATOR:
            self.read_uint32(&extended_num_bytes)
            num_bytes[0] = <ssize_t> extended_num_bytes
        ptr[0] = self._get_raw(num_bytes[0])

    cdef int _write_raw_bytes_and_length(self, const char_type *ptr,
                                         ssize_t num_bytes) except -1:
        """
        Helper function that writes the length in the format required before
        writing the bytes.
        """
        self.write_length(num_bytes)
        self.write_raw(ptr, <uint32_t> num_bytes)

    cdef int get_is_atomic_null(self, bint* is_null) except -1:
        """
        Reads the next byte and checks to see if the value is atomically null.
        If not, the byte is returned to the buffer for further processing.
        """
        cdef uint8_t value
        self.read_ub1(&value)
        if value in (TNS_OBJ_ATOMIC_NULL, TNS_NULL_LENGTH_INDICATOR):
            is_null[0] = True
        else:
            is_null[0] = False
            self._pos -= 1

    cdef int read_header(self, uint8_t* flags, uint8_t *version) except -1:
        """
        Reads the header of the pickled data.
        """
        cdef:
            uint32_t prefix_seg_length
            uint8_t tmp
        self.read_ub1(flags)
        self.read_ub1(version)
        self.skip_length()
        if flags[0] & TNS_OBJ_NO_PREFIX_SEG:
            return 0
        self.read_length(&prefix_seg_length)
        self.skip_raw_bytes(prefix_seg_length)

    cdef int read_length(self, uint32_t *length) except -1:
        """
        Read the length from the buffer. This will be a single byte, unless the
        value meets or exceeds TNS_LONG_LENGTH_INDICATOR. In that case, the
        value is stored as a 4-byte integer.
        """
        cdef uint8_t short_length
        self.read_ub1(&short_length)
        if short_length == TNS_LONG_LENGTH_INDICATOR:
            self.read_uint32(length)
        else:
            length[0] = short_length

    cdef object read_xmltype(self, BaseThinConnImpl conn_impl):
        """
        Reads an XML type from the buffer. This is similar to reading a
        database object but with specialized processing.
        """
        cdef:
            uint8_t image_flags, image_version
            BaseThinLobImpl lob_impl
            const char_type *ptr
            ssize_t bytes_left
            uint32_t xml_flag
            type cls
        self.read_header(&image_flags, &image_version)
        self.skip_raw_bytes(1)              # XML version
        self.read_uint32(&xml_flag)
        if xml_flag & TNS_XML_TYPE_FLAG_SKIP_NEXT_4:
            self.skip_raw_bytes(4)
        bytes_left = self.bytes_left()
        ptr = self.read_raw_bytes(bytes_left)
        if xml_flag & TNS_XML_TYPE_STRING:
            return ptr[:bytes_left].decode()
        elif xml_flag & TNS_XML_TYPE_LOB:
            lob_impl = conn_impl._create_lob_impl(DB_TYPE_CLOB,
                                                  ptr[:bytes_left])
            cls = PY_TYPE_ASYNC_LOB \
                if conn_impl._protocol._transport._is_async \
                else PY_TYPE_LOB
            return cls._from_impl(lob_impl)
        errors._raise_err(errors.ERR_UNEXPECTED_XML_TYPE, flag=xml_flag)

    cdef int skip_length(self) except -1:
        """
        Skips the length instead of reading it from the buffer.
        """
        cdef uint8_t short_length
        self.read_ub1(&short_length)
        if short_length == TNS_LONG_LENGTH_INDICATOR:
            self.skip_raw_bytes(4)

    cdef int write_header(self, ThinDbObjectImpl obj_impl) except -1:
        """
        Writes the header of the pickled data. Since the size is unknown at
        this point, zero is written initially and the actual size is written
        later.
        """
        cdef ThinDbObjectTypeImpl typ_impl = obj_impl.type
        self.write_uint8(obj_impl.image_flags)
        self.write_uint8(obj_impl.image_version)
        self.write_uint8(TNS_LONG_LENGTH_INDICATOR)
        self.write_uint32(0)
        if typ_impl.is_collection:
            self.write_uint8(1)             # length of prefix segment
            self.write_uint8(1)             # prefix segment contents

    cdef int write_length(self, ssize_t length) except -1:
        """
        Writes the length to the buffer.
        """
        if length <= TNS_OBJ_MAX_SHORT_LENGTH:
            self.write_uint8(<uint8_t> length)
        else:
            self.write_uint8(TNS_LONG_LENGTH_INDICATOR)
            self.write_uint32(<uint32_t> length)


@cython.final
cdef class TDSBuffer(Buffer):
    pass


cdef class ThinDbObjectImpl(BaseDbObjectImpl):
    cdef:
        uint8_t image_flags, image_version
        bytes toid, oid, packed_data
        uint32_t num_elements
        dict unpacked_assoc_array
        list unpacked_assoc_keys
        dict unpacked_attrs
        list unpacked_array
        uint16_t flags

    cdef inline int _ensure_assoc_keys(self) except -1:
        """
        Ensure that the keys for the associative array have been calculated.
        PL/SQL associative arrays keep their keys in sorted order so this must
        be calculated when indices are required.
        """
        if self.unpacked_assoc_keys is None:
            self.unpacked_assoc_keys = list(sorted(self.unpacked_assoc_array))

    cdef inline int _ensure_unpacked(self) except -1:
        """
        Ensure that the data has been unpacked.
        """
        if self.packed_data is not None:
            self._unpack_data()

    cdef bytes _get_packed_data(self):
        """
        Returns the packed data for the object. This will either be the value
        retrieved from the database or generated packed data (for new objects
        and those that have had their data unpacked already).
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl = self.type
            DbObjectPickleBuffer buf
            ssize_t size
        if self.packed_data is not None:
            return self.packed_data
        buf = DbObjectPickleBuffer.__new__(DbObjectPickleBuffer)
        buf._initialize()
        buf.write_header(self)
        self._pack_data(buf)
        size = buf._pos
        buf.skip_to(3)
        buf.write_uint32(size)
        return buf._data[:size]

    cdef int _pack_data(self, DbObjectPickleBuffer buf) except -1:
        """
        Packs the data from the object into the buffer.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl = self.type
            ThinDbObjectAttrImpl attr
            int32_t index
            object value
        if typ_impl.is_collection:
            buf.write_uint8(typ_impl.collection_flags)
            if typ_impl.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                self._ensure_assoc_keys()
                buf.write_length(len(self.unpacked_assoc_keys))
                for index in self.unpacked_assoc_keys:
                    buf.write_uint32(<uint32_t> index)
                    self._pack_value(buf, typ_impl.element_dbtype,
                                     typ_impl.element_objtype,
                                     self.unpacked_assoc_array[index])
            else:
                buf.write_length(len(self.unpacked_array))
                for value in self.unpacked_array:
                    self._pack_value(buf, typ_impl.element_dbtype,
                                    typ_impl.element_objtype, value)
        else:
            for attr in typ_impl.attrs:
                self._pack_value(buf, attr.dbtype, attr.objtype,
                                 self.unpacked_attrs[attr.name])

    cdef int _pack_value(self, DbObjectPickleBuffer buf,
                         DbType dbtype, ThinDbObjectTypeImpl objtype,
                         object value) except -1:
        """
        Packs a value into the buffer. At this point it is assumed that the
        value matches the correct type.
        """
        cdef:
            uint8_t ora_type_num = dbtype._ora_type_num
            ThinDbObjectImpl obj_impl
            BaseThinLobImpl lob_impl
            bytes temp_bytes
        if value is None:
            if objtype is not None and not objtype.is_collection:
                buf.write_uint8(TNS_OBJ_ATOMIC_NULL)
            else:
                buf.write_uint8(TNS_NULL_LENGTH_INDICATOR)
        elif ora_type_num in (TNS_DATA_TYPE_CHAR, TNS_DATA_TYPE_VARCHAR):
            if dbtype._csfrm == CS_FORM_IMPLICIT:
                temp_bytes = (<str> value).encode()
            else:
                temp_bytes = (<str> value).encode(ENCODING_UTF16)
            buf.write_bytes_with_length(temp_bytes)
        elif ora_type_num == TNS_DATA_TYPE_NUMBER:
            temp_bytes = (<str> cpython.PyObject_Str(value)).encode()
            buf.write_oracle_number(temp_bytes)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_INTEGER:
            buf.write_uint8(4)
            buf.write_uint32(<uint32_t> value)
        elif ora_type_num == TNS_DATA_TYPE_RAW:
            buf.write_bytes_with_length(value)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_DOUBLE:
            buf.write_binary_double(value)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_FLOAT:
            buf.write_binary_float(value)
        elif ora_type_num == TNS_DATA_TYPE_BOOLEAN:
            buf.write_uint8(4)
            buf.write_uint32(value)
        elif ora_type_num in (TNS_DATA_TYPE_DATE, TNS_DATA_TYPE_TIMESTAMP,
                              TNS_DATA_TYPE_TIMESTAMP_TZ,
                              TNS_DATA_TYPE_TIMESTAMP_LTZ):
            buf.write_oracle_date(value, dbtype._buffer_size_factor)
        elif ora_type_num in (TNS_DATA_TYPE_CLOB, TNS_DATA_TYPE_BLOB):
            lob_impl = <BaseThinLobImpl> value._impl
            buf.write_bytes_with_length(lob_impl._locator)
        elif ora_type_num == TNS_DATA_TYPE_INT_NAMED:
            obj_impl = value._impl
            if self.type.is_collection or obj_impl.type.is_collection:
                temp_bytes = obj_impl._get_packed_data()
                buf.write_bytes_with_length(temp_bytes)
            else:
                obj_impl._pack_data(buf)
        else:
            errors._raise_err(errors.ERR_DB_TYPE_NOT_SUPPORTED,
                              name=dbtype.name)

    cdef int _unpack_data(self) except -1:
        """
        Unpacks the packed data into a dictionary of Python values.
        """
        cdef DbObjectPickleBuffer buf
        buf = DbObjectPickleBuffer.__new__(DbObjectPickleBuffer)
        buf._populate_from_bytes(self.packed_data)
        buf.read_header(&self.image_flags, &self.image_version)
        self._unpack_data_from_buf(buf)
        self.packed_data = None

    cdef int _unpack_data_from_buf(self, DbObjectPickleBuffer buf) except -1:
        """
        Unpacks the data from the buffer into Python values.
        """
        cdef:
            dict unpacked_attrs = {}, unpacked_assoc_array = None
            ThinDbObjectTypeImpl typ_impl = self.type
            list unpacked_array = None
            ThinDbObjectAttrImpl attr
            uint32_t num_elements, i
            int32_t assoc_index
            object value
        if typ_impl.is_collection:
            if typ_impl.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                unpacked_assoc_array = {}
            else:
                unpacked_array = []
            buf.skip_raw_bytes(1)           # collection flags
            buf.read_length(&num_elements)
            for i in range(num_elements):
                if typ_impl.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                    buf.read_int32(&assoc_index)
                value = self._unpack_value(
                    buf,
                    typ_impl.element_dbtype,
                    typ_impl.element_objtype,
                    typ_impl._element_preferred_num_type
                )
                if typ_impl.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                    unpacked_assoc_array[assoc_index] = value
                else:
                    unpacked_array.append(value)
        else:
            unpacked_attrs = {}
            for attr in typ_impl.attrs:
                value = self._unpack_value(buf, attr.dbtype, attr.objtype,
                                           attr._preferred_num_type)
                unpacked_attrs[attr.name] = value
        self.unpacked_attrs = unpacked_attrs
        self.unpacked_array = unpacked_array
        self.unpacked_assoc_array = unpacked_assoc_array

    cdef object _unpack_value(self, DbObjectPickleBuffer buf,
                              DbType dbtype, ThinDbObjectTypeImpl objtype,
                              int preferred_num_type):
        """
        Unpacks a single value and returns it.
        """
        cdef:
            uint8_t ora_type_num = dbtype._ora_type_num
            uint8_t csfrm = dbtype._csfrm
            DbObjectPickleBuffer xml_buf
            BaseThinConnImpl conn_impl
            ThinDbObjectImpl obj_impl
            BaseThinLobImpl lob_impl
            bytes locator
            bint is_null
            type cls
        if ora_type_num == TNS_DATA_TYPE_NUMBER:
            return buf.read_oracle_number(preferred_num_type)
        elif ora_type_num == TNS_DATA_TYPE_BINARY_INTEGER:
            return buf.read_binary_integer()
        elif ora_type_num in (TNS_DATA_TYPE_VARCHAR, TNS_DATA_TYPE_CHAR):
            if csfrm == CS_FORM_NCHAR:
                conn_impl = self.type._conn_impl
                conn_impl._protocol._caps._check_ncharset_id()
            return buf.read_str(csfrm)
        elif ora_type_num == TNS_DATA_TYPE_RAW:
            return buf.read_bytes()
        elif ora_type_num == TNS_DATA_TYPE_BINARY_DOUBLE:
            return buf.read_binary_double()
        elif ora_type_num == TNS_DATA_TYPE_BINARY_FLOAT:
            return buf.read_binary_float()
        elif ora_type_num in (TNS_DATA_TYPE_DATE, TNS_DATA_TYPE_TIMESTAMP,
                              TNS_DATA_TYPE_TIMESTAMP_LTZ,
                              TNS_DATA_TYPE_TIMESTAMP_TZ):
            return buf.read_date()
        elif ora_type_num in (TNS_DATA_TYPE_CLOB,
                              TNS_DATA_TYPE_BLOB,
                              TNS_DATA_TYPE_BFILE):
            conn_impl = self.type._conn_impl
            locator = buf.read_bytes()
            if locator is None:
                return None
            lob_impl = conn_impl._create_lob_impl(dbtype, locator)
            cls = PY_TYPE_ASYNC_LOB \
                    if conn_impl._protocol._transport._is_async \
                    else PY_TYPE_LOB
            return cls._from_impl(lob_impl)
        elif ora_type_num == TNS_DATA_TYPE_BOOLEAN:
            return buf.read_bool()
        elif ora_type_num == TNS_DATA_TYPE_INT_NAMED:
            buf.get_is_atomic_null(&is_null)
            if is_null:
                return None
            if objtype is None:
                xml_buf = DbObjectPickleBuffer.__new__(DbObjectPickleBuffer)
                xml_buf._populate_from_bytes(buf.read_bytes())
                return xml_buf.read_xmltype(self.type._conn_impl)
            obj_impl = ThinDbObjectImpl.__new__(ThinDbObjectImpl)
            obj_impl.type = objtype
            if objtype.is_collection or self.type.is_collection:
                obj_impl.packed_data = buf.read_bytes()
            else:
                obj_impl._unpack_data_from_buf(buf)
            return PY_TYPE_DB_OBJECT._from_impl(obj_impl)
        errors._raise_err(errors.ERR_DB_TYPE_NOT_SUPPORTED, name=dbtype.name)

    def append_checked(self, object value):
        """
        Internal method for appending a value to a collection object.
        """
        cdef:
            ThinDbObjectTypeImpl typ_impl
            int32_t new_index
        self._ensure_unpacked()
        if self.unpacked_array is not None:
            typ_impl = self.type
            if typ_impl.max_num_elements > 0 \
                    and len(self.unpacked_array) >= typ_impl.max_num_elements:
                errors._raise_err(errors.ERR_INVALID_COLL_INDEX_SET,
                                  index=len(self.unpacked_array),
                                  min_index=0,
                                  max_index=typ_impl.max_num_elements - 1)
            self.unpacked_array.append(value)
        else:
            self._ensure_assoc_keys()
            new_index = self.unpacked_assoc_keys[-1] + 1 \
                    if self.unpacked_assoc_keys else 0
            self.unpacked_assoc_array[new_index] = value
            self.unpacked_assoc_keys.append(new_index)

    def copy(self):
        """
        Internal method for creating a copy of an object.
        """
        cdef ThinDbObjectImpl copied_impl
        copied_impl = ThinDbObjectImpl.__new__(ThinDbObjectImpl)
        copied_impl.type = self.type
        copied_impl.flags = self.flags
        copied_impl.image_flags = self.image_flags
        copied_impl.image_version = self.image_version
        copied_impl.toid = self.toid
        copied_impl.packed_data = self.packed_data
        copied_impl.num_elements = self.num_elements
        if self.unpacked_attrs is not None:
            copied_impl.unpacked_attrs = self.unpacked_attrs.copy()
        if self.unpacked_array is not None:
            copied_impl.unpacked_array = list(self.unpacked_array)
        return copied_impl

    def delete_by_index(self, int32_t index):
        """
        Internal method for deleting an entry from a collection that is indexed
        by integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array is not None:
            del self.unpacked_array[index]
        else:
            self.unpacked_assoc_keys = None
            del self.unpacked_assoc_array[index]

    def exists_by_index(self, int32_t index):
        """
        Internal method for determining if an entry exists in a collection that
        is indexed by integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array is not None:
            return index >= 0 and index < len(self.unpacked_array)
        else:
            return index in self.unpacked_assoc_array

    def get_attr_value(self, ThinDbObjectAttrImpl attr):
        """
        Internal method for getting an attribute value.
        """
        self._ensure_unpacked()
        return self.unpacked_attrs[attr.name]

    def get_element_by_index(self, int32_t index):
        """
        Internal method for getting an entry from a collection that is indexed
        by integers.
        """
        self._ensure_unpacked()
        try:
            if self.unpacked_array is not None:
                return self.unpacked_array[index]
            else:
                return self.unpacked_assoc_array[index]
        except (KeyError, IndexError):
            errors._raise_err(errors.ERR_INVALID_COLL_INDEX_GET, index=index)

    def get_first_index(self):
        """
        Internal method for getting the first index from a collection that is
        indexed by integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array:
            return 0
        elif self.unpacked_assoc_array:
            self._ensure_assoc_keys()
            return self.unpacked_assoc_keys[0]

    def get_last_index(self):
        """
        Internal method for getting the last index from a collection that is
        indexed by integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array:
            return len(self.unpacked_array) - 1
        elif self.unpacked_assoc_array:
            self._ensure_assoc_keys()
            return self.unpacked_assoc_keys[-1]

    def get_next_index(self, int32_t index):
        """
        Internal method for getting the next index from a collection that is
        indexed by integers.
        """
        cdef int32_t i
        self._ensure_unpacked()
        if self.unpacked_array:
            if index + 1 < len(self.unpacked_array):
                return index + 1
        elif self.unpacked_assoc_array:
            self._ensure_assoc_keys()
            for i in self.unpacked_assoc_keys:
                if i > index:
                    return i

    def get_prev_index(self, int32_t index):
        """
        Internal method for getting the next index from a collection that is
        indexed by integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array:
            if index > 0:
                return index - 1
        elif self.unpacked_assoc_array:
            self._ensure_assoc_keys()
            for i in reversed(self.unpacked_assoc_keys):
                if i < index:
                    return i

    def get_size(self):
        """
        Internal method for getting the size of a collection.
        """
        self._ensure_unpacked()
        if self.unpacked_array is not None:
            return len(self.unpacked_array)
        else:
            return len(self.unpacked_assoc_array)

    def set_attr_value_checked(self, ThinDbObjectAttrImpl attr, object value):
        """
        Internal method for setting an attribute value.
        """
        self._ensure_unpacked()
        self.unpacked_attrs[attr.name] = value

    def set_element_by_index_checked(self, int32_t index, object value):
        """
        Internal method for setting an entry in a collection that is indexed by
        integers.
        """
        self._ensure_unpacked()
        if self.unpacked_array is not None:
            try:
                self.unpacked_array[index] = value
            except IndexError:
                max_index = max(len(self.unpacked_array) - 1, 0)
                errors._raise_err(errors.ERR_INVALID_COLL_INDEX_SET,
                                  index=index, min_index=0,
                                  max_index=max_index)
        else:
            if index not in self.unpacked_assoc_array:
                self.unpacked_assoc_keys = None
            self.unpacked_assoc_array[index] = value

    def trim(self, int32_t num_to_trim):
        """
        Internal method for trimming a number of entries from a collection.
        """
        self._ensure_unpacked()
        if num_to_trim > 0:
            self.unpacked_array = self.unpacked_array[:-num_to_trim]


cdef class ThinDbObjectAttrImpl(BaseDbObjectAttrImpl):
    cdef:
        bytes oid


cdef class ThinDbObjectTypeImpl(BaseDbObjectTypeImpl):
    cdef:
        uint8_t collection_type, collection_flags, version
        uint32_t max_num_elements
        bint is_row_type
        bint is_xml_type
        bytes oid

    def create_new_object(self):
        """
        Internal method for creating a new object.
        """
        cdef ThinDbObjectImpl obj_impl
        obj_impl = ThinDbObjectImpl.__new__(ThinDbObjectImpl)
        obj_impl.type = self
        obj_impl.toid = b'\x00\x22' + \
                bytes([TNS_OBJ_NON_NULL_OID, TNS_OBJ_HAS_EXTENT_OID]) + \
                self.oid + TNS_EXTENT_OID
        obj_impl.flags = TNS_OBJ_TOP_LEVEL
        obj_impl.image_flags = TNS_OBJ_IS_VERSION_81
        obj_impl.image_version = TNS_OBJ_IMAGE_VERSION
        obj_impl.unpacked_attrs = {}
        if self.is_collection:
            obj_impl.image_flags |= TNS_OBJ_IS_COLLECTION
            if self.collection_type == TNS_OBJ_PLSQL_INDEX_TABLE:
                obj_impl.unpacked_assoc_array = {}
            else:
                obj_impl.unpacked_array = []
        else:
            obj_impl.image_flags |= TNS_OBJ_NO_PREFIX_SEG
            for attr in self.attrs:
                obj_impl.unpacked_attrs[attr.name] = None
        return obj_impl
